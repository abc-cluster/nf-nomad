package nextflow.nomad.executor

import groovy.util.logging.Slf4j
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import nextflow.exception.ProcessSubmitException
import nextflow.processor.TaskRun
import nextflow.util.Duration

import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.Properties
import java.net.HttpURLConnection
import java.net.URL

@Slf4j
class RcloneNomadInterop {

    private static final long DEFAULT_COMPLETION_TIMEOUT_MS = 60_000L
    private static final String DEFAULT_CONFIG_DELIVERY = 'hostPath'
    private static final String DEFAULT_SYNC_BACK = 'all'
    private static final String DEFAULT_TRANSFER_MODE = 'task'
    private static final String TRANSFER_MODE_TASK = 'task'
    private static final String TRANSFER_MODE_SIDECAR = 'sidecar'
    private static final String SIDECAR_DRIVER_RAW_EXEC = 'raw_exec'
    private static final String SIDECAR_DRIVER_DOCKER = 'docker'
    private static final String RUNTIME_METADATA_FILE = '.nf-rclone/runtime.properties'

    private final TaskRun task
    private final Map sessionConfig
    private final Path sessionWorkDir

    final boolean enabled
    final String remote
    final String remotePath
    final String configDelivery
    final String syncBack
    final String transferMode
    final String sidecarDriver
    final String sidecarImage
    final String sidecarUser
    final long completionTimeoutMillis
    final String rcloneConfigPath
    final boolean legalTransferEnabled
    final String policyServiceUrl
    final boolean policyFailOpen
    final int policyTimeoutMillis

    private List<String> submitCommand = Collections.emptyList()
    private Map<String, String> submitEnv = Collections.emptyMap()
    private List<NomadLifecycleTaskSpec> lifecycleTasks = Collections.emptyList()

    RcloneNomadInterop(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
        this.task = task
        this.sessionConfig = sessionConfig ?: Collections.emptyMap()
        this.sessionWorkDir = sessionWorkDir

        final Map rcloneScope = readMap(this.sessionConfig, 'rclone')
        final Map workScope = readMap(rcloneScope, 'rcloneWorkDir')
        this.enabled = toBoolean(workScope.get('enabled'))
        this.remote = toText(workScope.get('remote'))
        this.remotePath = toText(workScope.get('remotePath'))
        this.configDelivery = normalizeConfigDelivery(toText(workScope.get('configDelivery')))
        this.syncBack = normalizeSyncBack(toText(workScope.get('syncBack')))
        this.transferMode = normalizeTransferMode(toText(workScope.get('transferMode')))
        this.sidecarDriver = normalizeSidecarDriver(toText(workScope.get('sidecarDriver')))
        this.sidecarImage = toText(workScope.get('sidecarImage'))
        this.sidecarUser = toText(workScope.get('sidecarUser'))
        this.completionTimeoutMillis = parseDurationMillis(workScope.get('completionTimeout'), DEFAULT_COMPLETION_TIMEOUT_MS)
        this.rcloneConfigPath = resolveRcloneConfigPath(sessionWorkDir, rcloneScope)
        final Map legalScope = readMap(workScope, 'legalTransfer')
        this.legalTransferEnabled = toBoolean(legalScope.get('enabled'))
        this.policyServiceUrl = toText(legalScope.get('policyServiceUrl'))
        this.policyFailOpen = toBoolean(legalScope.containsKey('failOpen') ? legalScope.get('failOpen') : true)
        this.policyTimeoutMillis = parseInteger(legalScope.get('timeoutMillis'), 5000)
    }

    List<String> getSubmitCommand() {
        return submitCommand
    }

    Map<String, String> getSubmitEnv() {
        return submitEnv
    }

    List<NomadLifecycleTaskSpec> getLifecycleTasks() {
        return lifecycleTasks
    }

    String getRemoteExitLocation() {
        return remoteExitFile()
    }

    Object createCopyStrategy(TaskRun taskRun) {
        return createCopyStrategy(taskRun, false)
    }

    /**
     * Create an nf-rclone copy strategy for the BashWrapperBuilder.
     *
     * @param taskRun          the Nextflow task
     * @param stagingDisabled  when true, the strategy returns empty stage-in/out scripts
     *                         (used in sidecar mode where lifecycle tasks handle staging)
     */
    Object createCopyStrategy(TaskRun taskRun, boolean stagingDisabled) {
        if( !enabled || !rcloneConfigPath || taskRun == null ) {
            return null
        }
        try {
            final ClassLoader loader = this.class.classLoader
            final Class rcloneConfigClass = loader.loadClass('nextflow.rclone.config.RcloneConfig')
            final Class strategyClass = loader.loadClass('nextflow.rclone.strategy.RcloneFileCopyStrategy')
            final Map rcloneScope = readMap(sessionConfig, 'rclone')
            final Object rcloneConfig = rcloneConfigClass.getMethod('fromMap', Map).invoke(null, rcloneScope)
            try {
                // Try the 7-arg constructor (with stagingDisabled flag)
                return strategyClass
                        .getConstructor(rcloneConfigClass, String, Path, Path, String, String, boolean)
                        .newInstance(
                                rcloneConfig,
                                rcloneConfigPath,
                                taskRun.workDir,
                                taskRun.targetDir,
                                taskRun.config?.getStageInMode(),
                                taskRun.config?.getStageOutMode(),
                                stagingDisabled
                        )
            }
            catch (NoSuchMethodException ignored1) {
                try {
                    // Fall back to 6-arg constructor (no stagingDisabled)
                    return strategyClass
                            .getConstructor(rcloneConfigClass, String, Path, Path, String, String)
                            .newInstance(
                                    rcloneConfig,
                                    rcloneConfigPath,
                                    taskRun.workDir,
                                    taskRun.targetDir,
                                    taskRun.config?.getStageInMode(),
                                    taskRun.config?.getStageOutMode()
                            )
                }
                catch (NoSuchMethodException ignored2) {
                    return strategyClass
                            .getConstructor(rcloneConfigClass, String)
                            .newInstance(rcloneConfig, rcloneConfigPath)
                }
            }
        }
        catch (ClassNotFoundException e) {
            log.debug("[NOMAD] nf-rclone strategy classes are not available in classpath; falling back to Nomad default copy strategy")
            return null
        }
        catch (Throwable e) {
            log.warn("[NOMAD] Unable to initialize nf-rclone copy strategy; falling back to Nomad default strategy -- ${e.message ?: e}")
            return null
        }
    }

    /**
     * Whether this interop is configured for sidecar (lifecycle task) mode.
     */
    boolean isSidecarMode() {
        return transferMode == TRANSFER_MODE_SIDECAR
    }

    void prepare() {
        if( !enabled ) {
            return
        }
        validateConfiguration()
        uploadCommandFiles()

        if( transferMode == TRANSFER_MODE_SIDECAR ) {
            buildSidecarSubmission()
        }
        else {
            buildBootstrapSubmission()
        }
        enforceLegalTransferPolicy()
    }

    Integer synchronizeCompletion() {
        if( !enabled ) {
            return null
        }
        validateConfiguration()
        String policyViolation = readRemotePolicyViolationMessage()
        if( policyViolation ) {
            throw new ProcessSubmitException("[NOMAD] Transfer policy violation: ${policyViolation}")
        }

        Integer remoteExit = awaitRemoteExitCode()
        if( syncBack == 'none' ) {
            copyCommandMetadata()
        }
        else {
            copyAllArtifacts()
        }

        // In sidecar mode, verify that the poststop task completed its transfers.
        // The poststop script writes .rclone-poststop-exitcode on success.
        if( transferMode == TRANSFER_MODE_SIDECAR && remoteExit != null && remoteExit == 0 ) {
            Integer poststopExit = readRemotePoststopExitCode()
            if( poststopExit == null ) {
                log.warn("[NOMAD] Main task exited 0 but poststop transfer marker is missing at `${remotePoststopExitFile()}` — poststop may have failed")
                // Write a local marker so downstream can distinguish this case
                writeLocalPoststopWarning()
            }
        }

        if( remoteExit != null ) {
            writeLocalExitCode(remoteExit)
        }
        return remoteExit
    }

    protected void validateConfiguration() {
        if( !remote ) {
            throw new ProcessSubmitException('[NOMAD] nf-rclone interop enabled but `rclone.rcloneWorkDir.remote` is missing')
        }
        if( !remotePath ) {
            throw new ProcessSubmitException('[NOMAD] nf-rclone interop enabled but `rclone.rcloneWorkDir.remotePath` is missing')
        }
        if( !rcloneConfigPath && configDelivery == 'hostPath' ) {
            throw new ProcessSubmitException('[NOMAD] nf-rclone interop enabled but rclone config path could not be resolved')
        }
        if( configDelivery == 'hostPath' && !Files.exists(Path.of(rcloneConfigPath)) ) {
            throw new ProcessSubmitException("[NOMAD] nf-rclone interop could not find rclone config at `${rcloneConfigPath}`")
        }
        if( configDelivery == 'inline' && !rcloneConfigPath ) {
            throw new ProcessSubmitException('[NOMAD] nf-rclone interop inline config delivery requires a readable rclone config path')
        }
        if( transferMode == TRANSFER_MODE_SIDECAR && sidecarDriver == SIDECAR_DRIVER_DOCKER && !sidecarImage ) {
            throw new ProcessSubmitException('[NOMAD] nf-rclone sidecar driver `docker` requires `rclone.rcloneWorkDir.sidecarImage`')
        }
        if( legalTransferEnabled && !policyServiceUrl ) {
            throw new ProcessSubmitException('[NOMAD] legal transfer policy is enabled but `rclone.rcloneWorkDir.legalTransfer.policyServiceUrl` is missing')
        }
    }

    protected void uploadCommandFiles() {
        final List<String> cmd = [
                'rclone', 'copy',
                '--config', rcloneConfigPath,
                '--retries', '3',
                '--low-level-retries', '10',
                '--include', '.command.*',
                task.workDir.toString(),
                remoteTaskDir()
        ]
        final CommandResult result = runCommand(cmd)
        if( result.exitCode != 0 ) {
            throw new ProcessSubmitException("[NOMAD] Failed uploading task scripts to remote backend `${remoteTaskDir()}` -- ${result.stderr ?: result.stdout}")
        }
    }

    protected void buildBootstrapSubmission() {
        final Map<String, String> env = buildTransferEnv()
        submitEnv = env
        submitCommand = ['bash', '-c', bootstrapScript()]
        lifecycleTasks = Collections.emptyList()
    }

    protected void buildSidecarSubmission() {
        final Map<String, String> transferEnv = buildTransferEnv()
        final String lifecycleDriver = sidecarDriver ?: SIDECAR_DRIVER_RAW_EXEC
        final String lifecycleUser = lifecycleDriver == SIDECAR_DRIVER_RAW_EXEC ? sidecarUser : null
        final Map<String, Object> lifecycleConfig = buildLifecycleDriverConfig(lifecycleDriver)

        // --- attempt standalone script generation via nf-rclone ---
        String prestartScriptContent
        String poststopScriptContent
        String prestartManifestJson = null
        String poststopManifestJson = null

        try {
            def stageResult = generateStandaloneStageIn()
            if( stageResult != null ) {
                prestartScriptContent = readProperty(stageResult, 'script')?.toString()
                def manifest = readProperty(stageResult, 'manifest')
                if( manifest != null ) {
                    prestartManifestJson = invokeMethod(manifest, 'toJson')?.toString()
                }
            }
        }
        catch (Exception e) {
            log.debug("[NOMAD] Standalone stage-in generation not available, falling back to legacy prestart -- ${e.message ?: e}")
            prestartScriptContent = null
        }

        try {
            def stageResult = generateStandaloneStageOut()
            if( stageResult != null ) {
                poststopScriptContent = readProperty(stageResult, 'script')?.toString()
                def manifest = readProperty(stageResult, 'manifest')
                if( manifest != null ) {
                    poststopManifestJson = invokeMethod(manifest, 'toJson')?.toString()
                }
            }
        }
        catch (Exception e) {
            log.debug("[NOMAD] Standalone stage-out generation not available, falling back to legacy poststop -- ${e.message ?: e}")
            poststopScriptContent = null
        }

        // Fall back to legacy scripts if standalone generation failed
        if( !prestartScriptContent ) {
            prestartScriptContent = prestartScript()
        }
        if( !poststopScriptContent ) {
            poststopScriptContent = poststopScript()
        }

        final List<String> prestartCommand = lifecycleDriver == SIDECAR_DRIVER_DOCKER
                ? ['-lc', prestartScriptContent]
                : ['bash', '-lc', prestartScriptContent]
        final List<String> poststopCommand = lifecycleDriver == SIDECAR_DRIVER_DOCKER
                ? ['-lc', poststopScriptContent]
                : ['bash', '-lc', poststopScriptContent]

        // --- build lifecycle task metadata ---
        final Map<String, String> prestartMeta = new LinkedHashMap<>()
        prestartMeta.put('nf.phase', 'prestart')
        prestartMeta.put('nf.taskHash', taskRemotePath())
        prestartMeta.put('nf.transferMode', 'sidecar')

        final Map<String, String> poststopMeta = new LinkedHashMap<>()
        poststopMeta.put('nf.phase', 'poststop')
        poststopMeta.put('nf.taskHash', taskRemotePath())
        poststopMeta.put('nf.transferMode', 'sidecar')

        final List<NomadLifecycleTaskSpec> specs = []

        specs << new NomadLifecycleTaskSpec(
                name: 'nf-rclone-prestart',
                hook: 'prestart',
                sidecar: false,
                driver: lifecycleDriver,
                user: lifecycleUser,
                command: prestartCommand,
                config: lifecycleConfig,
                env: transferEnv,
                cpu: 200,
                memoryMb: 128,
                transferManifest: prestartManifestJson,
                meta: prestartMeta
        )

        specs << new NomadLifecycleTaskSpec(
                name: 'nf-rclone-poststop',
                hook: 'poststop',
                sidecar: false,
                driver: lifecycleDriver,
                user: lifecycleUser,
                command: poststopCommand,
                config: lifecycleConfig,
                env: transferEnv,
                cpu: 200,
                memoryMb: 128,
                transferManifest: poststopManifestJson,
                meta: poststopMeta
        )

        submitCommand = ['bash', '-lc', sidecarMainTaskScript()]
        submitEnv = Collections.emptyMap()
        lifecycleTasks = specs
    }

    protected Map<String, Object> buildLifecycleDriverConfig(String lifecycleDriver) {
        if( lifecycleDriver == SIDECAR_DRIVER_DOCKER ) {
            final Map<String, Object> config = new LinkedHashMap<>()
            config.put('image', sidecarImage)
            config.put('entrypoint', ['sh'])
            config.put('network_mode', 'host')
            if( configDelivery == 'hostPath' && rcloneConfigPath ) {
                final String mount = rcloneConfigPath + ':' + rcloneConfigPath + ':ro'
                config.put('volumes', [mount])
            }
            return config
        }
        return Collections.emptyMap()
    }

    /**
     * Use nf-rclone's StandaloneStageScriptGenerator to produce a complete
     * prestart script with data staging commands and transfer manifest.
     * Returns null if nf-rclone classes are not available.
     */
    protected Object generateStandaloneStageIn() {
        final ClassLoader loader = this.class.classLoader
        final Class generatorClass = loader.loadClass('nextflow.rclone.strategy.StandaloneStageScriptGenerator')
        final Class rcloneConfigClass = loader.loadClass('nextflow.rclone.config.RcloneConfig')
        final Map rcloneScope = readMap(sessionConfig, 'rclone')
        final Object rcloneConfig = rcloneConfigClass.getMethod('fromMap', Map).invoke(null, rcloneScope)

        def generator = generatorClass.getConstructor(rcloneConfigClass).newInstance(rcloneConfig)

        // Collect task input files
        Map<String, java.nio.file.Path> inputFiles = Collections.emptyMap()
        try {
            inputFiles = task.getInputFilesMap() ?: Collections.emptyMap()
        }
        catch (Exception e) {
            log.debug("[NOMAD] Unable to retrieve task input files map: ${e.message}")
        }

        return generatorClass.getMethod('generateStageInScript', Map, String, String, String)
                .invoke(generator, inputFiles, remoteTaskDir(), taskRemotePath(), remote)
    }

    /**
     * Use nf-rclone's StandaloneStageScriptGenerator to produce a complete
     * poststop script with data staging commands and transfer manifest.
     * Returns null if nf-rclone classes are not available.
     */
    protected Object generateStandaloneStageOut() {
        final ClassLoader loader = this.class.classLoader
        final Class generatorClass = loader.loadClass('nextflow.rclone.strategy.StandaloneStageScriptGenerator')
        final Class rcloneConfigClass = loader.loadClass('nextflow.rclone.config.RcloneConfig')
        final Map rcloneScope = readMap(sessionConfig, 'rclone')
        final Object rcloneConfig = rcloneConfigClass.getMethod('fromMap', Map).invoke(null, rcloneScope)

        def generator = generatorClass.getConstructor(rcloneConfigClass).newInstance(rcloneConfig)

        // Collect task output file names
        List<String> outputFiles = Collections.emptyList()
        try {
            outputFiles = task.getOutputFilesNames() ?: Collections.emptyList()
        }
        catch (Exception e) {
            log.debug("[NOMAD] Unable to retrieve task output file names: ${e.message}")
        }

        return generatorClass.getMethod('generateStageOutScript', List, String, String, String)
                .invoke(generator, outputFiles, remoteTaskDir(), taskRemotePath(), remote)
    }

    protected Map<String, String> buildTransferEnv() {
        final Map<String, String> env = new LinkedHashMap<>()
        env.put('NXF_RCLONE_REMOTE_WORKDIR', remoteTaskDir())

        if( configDelivery == 'inline' ) {
            String encoded = Base64.encoder.encodeToString(Files.readAllBytes(Path.of(rcloneConfigPath)))
            env.put('NXF_RCLONE_CONFIG_B64', encoded)
        }
        else {
            env.put('NXF_RCLONE_CONFIG', rcloneConfigPath)
        }
        return env
    }

    protected Integer awaitRemoteExitCode() {
        final long deadline = System.currentTimeMillis() + completionTimeoutMillis
        while( System.currentTimeMillis() <= deadline ) {
            Integer exit = readRemoteExitCode()
            if( exit != null ) {
                return exit
            }
            sleepQuietly(1000L)
        }
        return null
    }

    protected Integer readRemoteExitCode() {
        final List<String> cmd = [
                'rclone', 'cat',
                '--config', rcloneConfigPath,
                '--retries', '3',
                '--low-level-retries', '10',
                remoteExitFile()
        ]
        final CommandResult result = runCommand(cmd)
        if( result.exitCode != 0 ) {
            return null
        }
        try {
            String value = result.stdout?.trim()
            return value ? Integer.parseInt(value) : null
        }
        catch (Exception e) {
            log.warn("[NOMAD] nf-rclone interop received invalid remote exit code `${result.stdout?.trim()}` for task `${task?.name}`")
            return null
        }
    }

    protected void copyAllArtifacts() {
        final List<String> cmd = [
                'rclone', 'copy',
                '--config', rcloneConfigPath,
                '--retries', '3',
                '--low-level-retries', '10',
                remoteTaskDir(),
                task.workDir.toString()
        ]
        final CommandResult result = runCommand(cmd)
        if( result.exitCode != 0 ) {
            throw new ProcessSubmitException("[NOMAD] Failed copying remote task artifacts from `${remoteTaskDir()}` -- ${result.stderr ?: result.stdout}")
        }
    }

    protected void copyCommandMetadata() {
        final List<String> cmd = [
                'rclone', 'copy',
                '--config', rcloneConfigPath,
                '--retries', '3',
                '--low-level-retries', '10',
                '--include', '.exitcode',
                '--include', '.command.*',
                remoteTaskDir(),
                task.workDir.toString()
        ]
        final CommandResult result = runCommand(cmd)
        if( result.exitCode != 0 ) {
            throw new ProcessSubmitException("[NOMAD] Failed copying remote command metadata from `${remoteTaskDir()}` -- ${result.stderr ?: result.stdout}")
        }
    }

    protected void writeLocalExitCode(Integer code) {
        if( code == null ) {
            return
        }
        Files.writeString(task.workDir.resolve(TaskRun.CMD_EXIT), String.valueOf(code))
    }

    protected String remoteTaskDir() {
        String base = remotePath.endsWith('/') ? remotePath : "${remotePath}/"
        return "${remote}:${base}${taskRemotePath()}/"
    }

    protected String remoteExitFile() {
        return "${remoteTaskDir()}.exitcode"
    }

    protected String remotePoststopExitFile() {
        return "${remoteTaskDir()}.rclone-poststop-exitcode"
    }

    protected Integer readRemotePoststopExitCode() {
        final List<String> cmd = [
                'rclone', 'cat',
                '--config', rcloneConfigPath,
                '--retries', '3',
                '--low-level-retries', '10',
                remotePoststopExitFile()
        ]
        final CommandResult result = runCommand(cmd)
        if( result.exitCode != 0 ) {
            return null
        }
        try {
            String value = result.stdout?.trim()
            return value ? Integer.parseInt(value) : null
        }
        catch (Exception e) {
            return null
        }
    }

    protected void writeLocalPoststopWarning() {
        try {
            Files.writeString(
                    task.workDir.resolve('.rclone-poststop-warning'),
                    'poststop transfer marker was not found on remote — outputs may be incomplete'
            )
        }
        catch (Exception ignored) {
            // best-effort warning
        }
    }

    protected String taskRemotePath() {
        String fromSession = relativePathFromSessionWorkDir(task.workDir, sessionWorkDir)
        if( !fromSession ) {
            throw new ProcessSubmitException(
                    "[NOMAD] nf-rclone interop requires task workDir `${task.workDir}` to be nested under session workDir `${sessionWorkDir}`"
            )
        }
        if( !isNextflowWorkPathLayout(fromSession) ) {
            throw new ProcessSubmitException(
                    "[NOMAD] nf-rclone interop requires Nextflow workDir layout `NN/HASH`; found `${fromSession}`"
            )
        }
        return fromSession
    }

    protected String taskHash() {
        String value = task.workDir?.fileName?.toString()
        if( value ) {
            return value
        }
        value = task.hash?.toString()
        if( value ) {
            return value
        }
        throw new ProcessSubmitException('[NOMAD] Unable to determine task hash for nf-rclone interop')
    }

    protected static String relativePathFromSessionWorkDir(Path taskWorkDir, Path sessionWorkDir) {
        if( taskWorkDir == null || sessionWorkDir == null ) {
            return null
        }
        try {
            Path taskAbs = taskWorkDir.toAbsolutePath().normalize()
            Path sessionAbs = sessionWorkDir.toAbsolutePath().normalize()
            if( !taskAbs.startsWith(sessionAbs) ) {
                return null
            }
            Path relative = sessionAbs.relativize(taskAbs)
            return normalizeRelativePath(relative)
        }
        catch (Exception ignored) {
            return null
        }
    }


    protected static String normalizeRelativePath(Path path) {
        if( path == null ) {
            return null
        }
        String value = path.toString().replace('\\' as char, '/' as char)
        value = value.replaceAll('^/+', '').replaceAll('/+$', '')
        if( !value ) {
            return null
        }
        if( value == '..' || value.startsWith('../') || value.contains('/../') ) {
            return null
        }
        return value
    }

    protected static boolean isNextflowWorkPathLayout(String value) {
        if( !value ) {
            return false
        }
        return value ==~ /[0-9a-fA-F]{2}\/[0-9a-fA-F]+/
    }


    protected CommandResult runCommand(List<String> command) {
        Process proc = command.execute()
        StringBuffer stdout = new StringBuffer()
        StringBuffer stderr = new StringBuffer()
        proc.consumeProcessOutput(stdout, stderr)
        proc.waitFor()
        return new CommandResult(proc.exitValue(), stdout.toString(), stderr.toString())
    }

    protected static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis)
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt()
            throw new ProcessSubmitException('[NOMAD] Interrupted while waiting for remote task completion', e)
        }
    }

    protected static String sidecarMainTaskScript() {
        return '''\
set -euo pipefail
TASK_DIR="$${NOMAD_ALLOC_DIR:-$${NOMAD_TASK_DIR:-$PWD}}/nf-rclone-task"
cd "$TASK_DIR"
if [ -f .command.run ]; then
  _task_script='.command.run'
elif [ -f .command.sh ]; then
  _task_script='.command.sh'
else
  echo \"[NOMAD] Missing .command.run/.command.sh in sidecar task directory: $TASK_DIR\" >&2
  exit 127
fi
set +e
unset NXF_CHDIR 2>/dev/null || true
if [ \"$_task_script\" = '.command.run' ]; then
  bash .command.run
else
  bash .command.sh > .command.out 2> .command.err
fi
_exit_code=$?
set -e
if [ ! -f .exitcode ]; then
  printf '%s' \"$_exit_code\" > .exitcode
fi
exit "$_exit_code"
'''.stripIndent()
    }

    protected static String prestartScript() {
        return '''\
set -eu
(set -o pipefail 2>/dev/null) || true
TASK_DIR="$${NOMAD_ALLOC_DIR:-$${NOMAD_TASK_DIR:-$PWD}}/nf-rclone-task"

mkdir -p "$TASK_DIR"

if [ -n "$${NXF_RCLONE_CONFIG_B64:-}" ]; then

  if printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode >/dev/null 2>&1; then
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode > "$TASK_DIR/.rclone.conf"
  else
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 -d > "$TASK_DIR/.rclone.conf"
  fi
  chmod 600 "$TASK_DIR/.rclone.conf" || true
  export NXF_RCLONE_CONFIG="$TASK_DIR/.rclone.conf"
fi

if [ -z "$${NXF_RCLONE_CONFIG:-}" ]; then


  echo "[NOMAD] Missing NXF_RCLONE_CONFIG for nf-rclone prestart transfer" >&2
  exit 127
fi

rclone copy --config "$NXF_RCLONE_CONFIG" --include '.command.*' "$${NXF_RCLONE_REMOTE_WORKDIR}/" "$TASK_DIR/"

if [ -f \"$TASK_DIR/.command.run\" ]; then
  awk '
    {
      gsub(/\\/[^[:space:]]*\\/\\.command\\.run/, \".command.run\")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.sh/, \".command.sh\")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.begin/, \".command.begin\")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.trace/, \".command.trace\")
      gsub(/\\/[^[:space:]]*\\/\\.exitcode/, \".exitcode\")
      print
    }
  ' \"$TASK_DIR/.command.run\" > \"$TASK_DIR/.command.run.patched\"
  mv \"$TASK_DIR/.command.run.patched\" \"$TASK_DIR/.command.run\"
fi
chmod 777 "$TASK_DIR" 2>/dev/null || true
for f in .command.run .command.sh .command.in .command.stub; do
  if [ -e "$TASK_DIR/$f" ]; then
    chmod 755 \"$TASK_DIR/$f\" 2>/dev/null || true
  fi
done
touch "$TASK_DIR/.command.out" "$TASK_DIR/.command.err" "$TASK_DIR/.exitcode" 2>/dev/null || true
chmod 666 "$TASK_DIR/.command.out" "$TASK_DIR/.command.err" "$TASK_DIR/.exitcode" 2>/dev/null || true
'''.stripIndent()
    }

    protected static String poststopScript() {
        return '''\
set -eu
(set -o pipefail 2>/dev/null) || true
TASK_DIR="$${NOMAD_ALLOC_DIR:-$${NOMAD_TASK_DIR:-$PWD}}/nf-rclone-task"
mkdir -p "$TASK_DIR"

if [ -n "$${NXF_RCLONE_CONFIG_B64:-}" ] && [ ! -s "$TASK_DIR/.rclone.conf" ]; then
  if printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode >/dev/null 2>&1; then
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode > "$TASK_DIR/.rclone.conf"
  else
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 -d > "$TASK_DIR/.rclone.conf"
  fi
  chmod 600 "$TASK_DIR/.rclone.conf" || true
fi

if [ -z "$${NXF_RCLONE_CONFIG:-}" ] && [ -s "$TASK_DIR/.rclone.conf" ]; then
  export NXF_RCLONE_CONFIG="$TASK_DIR/.rclone.conf"
fi

if [ -z "$${NXF_RCLONE_CONFIG:-}" ]; then

  echo "[NOMAD] Missing NXF_RCLONE_CONFIG for nf-rclone poststop transfer" >&2
  exit 127
fi
if [ ! -f "$TASK_DIR/.exitcode" ]; then
  printf '%s' '127' > "$TASK_DIR/.exitcode"
fi

rclone copy --config "$NXF_RCLONE_CONFIG" "$TASK_DIR/" "$${NXF_RCLONE_REMOTE_WORKDIR}/"
'''.stripIndent()
    }

    protected static String bootstrapScript() {
        return '''\
set -euo pipefail
TASK_DIR="$${NOMAD_TASK_DIR:-$PWD}/nf-rclone-task"
mkdir -p "$TASK_DIR"
cd "$TASK_DIR"

if [ -n "$${NXF_RCLONE_CONFIG_B64:-}" ]; then
  if printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode >/dev/null 2>&1; then
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode > .rclone.conf
  else
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 -d > .rclone.conf
  fi
  chmod 600 .rclone.conf || true
  export NXF_RCLONE_CONFIG="$PWD/.rclone.conf"
fi

if [ -z "$${NXF_RCLONE_CONFIG:-}" ]; then
  echo "[NOMAD] Missing NXF_RCLONE_CONFIG for nf-rclone bootstrap" >&2
  exit 127
fi

rclone copy --config "$NXF_RCLONE_CONFIG" --include '.command.*' "$${NXF_RCLONE_REMOTE_WORKDIR}/" ./

if [ -f .command.run ]; then
  awk '
    {
      gsub(/\\/[^[:space:]]*\\/\\.command\\.run/, \".command.run\")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.sh/, \".command.sh\")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.begin/, \".command.begin\")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.trace/, \".command.trace\")
      gsub(/\\/[^[:space:]]*\\/\\.exitcode/, \".exitcode\")
      print
    }
  ' .command.run > .command.run.patched
  mv .command.run.patched .command.run
  chmod 755 .command.run 2>/dev/null || true
fi

if [ -f .command.run ]; then
  _task_script='.command.run'
elif [ -f .command.sh ]; then
  _task_script='.command.sh'
else
  echo \"[NOMAD] Missing .command.run/.command.sh in bootstrap task directory: $TASK_DIR\" >&2
  exit 127
fi
set +e
unset NXF_CHDIR 2>/dev/null || true
bash \"$_task_script\"
_exit_code=$?
set -e
printf '%s' "$_exit_code" > .exitcode
rclone copy --config "$NXF_RCLONE_CONFIG" ./ "$${NXF_RCLONE_REMOTE_WORKDIR}/"
exit "$_exit_code"
'''.stripIndent()
    }

    protected static String resolveRcloneConfigPath(Path sessionWorkDir, Map rcloneScope) {
        String fromRuntime = readRuntimeMetadata(sessionWorkDir)?.getProperty('rclone.configPath')
        if( fromRuntime ) {
            return fromRuntime
        }

        if( sessionWorkDir != null ) {
            Path generated = sessionWorkDir.resolve('.nf-rclone').resolve('rclone-nf.conf')
            if( Files.exists(generated) ) {
                return generated.toString()
            }
        }

        String explicit = toText(rcloneScope?.get('rcloneConf'))
        return explicit ?: null
    }

    protected static Properties readRuntimeMetadata(Path sessionWorkDir) {
        if( sessionWorkDir == null ) {
            return null
        }
        Path metadata = sessionWorkDir.resolve(RUNTIME_METADATA_FILE)
        if( !Files.exists(metadata) ) {
            return null
        }
        try {
            Properties props = new Properties()
            Files.newInputStream(metadata).withCloseable { inStream ->
                props.load(inStream)
            }
            return props
        }
        catch (Exception ignored) {
            return null
        }
    }

    protected static String normalizeConfigDelivery(String value) {
        if( !value ) {
            return DEFAULT_CONFIG_DELIVERY
        }
        String normalized = value.trim()
        return (normalized == 'inline') ? 'inline' : DEFAULT_CONFIG_DELIVERY
    }

    protected static String normalizeSyncBack(String value) {
        if( !value ) {
            return DEFAULT_SYNC_BACK
        }
        String normalized = value.trim()
        if( normalized in ['all', 'declared', 'none'] ) {
            return normalized
        }
        return DEFAULT_SYNC_BACK
    }

    protected static String normalizeTransferMode(String value) {
        if( !value ) {
            return DEFAULT_TRANSFER_MODE
        }
        String normalized = value.trim().toLowerCase()
        return normalized == TRANSFER_MODE_SIDECAR ? TRANSFER_MODE_SIDECAR : TRANSFER_MODE_TASK
    }

    protected static String normalizeSidecarDriver(String value) {
        if( !value ) {
            return SIDECAR_DRIVER_RAW_EXEC
        }
        String normalized = value.trim().toLowerCase()
        return normalized == SIDECAR_DRIVER_DOCKER ? SIDECAR_DRIVER_DOCKER : SIDECAR_DRIVER_RAW_EXEC
    }

    protected static long parseDurationMillis(Object value, long defaultValue) {
        if( value == null ) {
            return defaultValue
        }
        try {
            if( value instanceof Duration ) {
                return (value as Duration).millis
            }
            return Duration.of(value.toString()).millis
        }
        catch (Exception ignored) {
            return defaultValue
        }
    }

    protected static boolean toBoolean(Object value) {
        if( value == null ) {
            return false
        }
        if( value instanceof Boolean ) {
            return (Boolean)value
        }
        return Boolean.valueOf(value.toString())
    }

    protected static String toText(Object value) {
        String text = value?.toString()?.trim()
        return text ? text : null
    }

    protected static Map readMap(Map source, String key) {
        if( source == null ) {
            return Collections.emptyMap()
        }
        Object value = source.get(key)
        return value instanceof Map ? (Map)value : Collections.emptyMap()
    }

    protected static Object readProperty(Object target, String property) {
        try {
            return org.codehaus.groovy.runtime.InvokerHelper.getProperty(target, property)
        } catch (Throwable ignored) {
            return null
        }
    }

    protected static String invokeMethod(Object target, String method) {
        try {
            return org.codehaus.groovy.runtime.InvokerHelper.invokeMethod(target, method, null)?.toString()
        } catch (Throwable ignored) {
            return null
        }
    }

    static class CommandResult {
        final int exitCode
        final String stdout
        final String stderr

        CommandResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode
            this.stdout = stdout
            this.stderr = stderr
        }
    }

    protected int parseInteger(Object value, int defaultValue) {
        if( value == null ) return defaultValue
        try {
            if( value instanceof Number ) return ((Number)value).intValue()
            return Integer.parseInt(value.toString())
        }
        catch (Exception ignored) {
            return defaultValue
        }
    }

    protected String remotePolicyViolationFile() {
        return "${remoteTaskDir()}.policy-violation.json"
    }

    protected String readRemotePolicyViolationMessage() {
        final List<String> cmd = [
                'rclone', 'cat',
                '--config', rcloneConfigPath,
                '--retries', '2',
                remotePolicyViolationFile()
        ]
        final CommandResult result = runCommand(cmd)
        if( result.exitCode != 0 || !result.stdout?.trim() ) {
            return null
        }
        try {
            def parsed = new JsonSlurper().parseText(result.stdout)
            return parsed?.reason?.toString() ?: parsed?.message?.toString() ?: result.stdout.trim()
        }
        catch (Exception ignored) {
            return result.stdout.trim()
        }
    }

    protected void writeRemotePolicyViolation(String reason, Map details = Collections.emptyMap()) {
        try {
            Path tmp = Files.createTempFile('nf-rclone-policy-violation-', '.json')
            def payload = new LinkedHashMap<String, Object>()
            payload.put('reason', reason ?: 'transfer rejected by policy')
            payload.put('task', task?.name)
            payload.put('remoteTaskDir', remoteTaskDir())
            payload.put('details', details ?: Collections.emptyMap())
            Files.writeString(tmp, JsonOutput.toJson(payload))
            final List<String> cmd = [
                    'rclone', 'copyto',
                    '--config', rcloneConfigPath,
                    tmp.toString(),
                    remotePolicyViolationFile()
            ]
            runCommand(cmd)
            Files.deleteIfExists(tmp)
        }
        catch (Exception e) {
            log.warn("[NOMAD] Unable to write remote policy-violation marker: ${e.message ?: e}")
        }
    }

    protected List<Map<String, Object>> collectTransferEntries() {
        final List<Map<String, Object>> out = []
        lifecycleTasks?.each { NomadLifecycleTaskSpec spec ->
            if( !spec?.transferManifest ) {
                return
            }
            try {
                def manifest = new JsonSlurper().parseText(spec.transferManifest)
                def transfers = manifest?.transfers instanceof List ? (List)manifest.transfers : Collections.emptyList()
                transfers.each { item ->
                    if( item instanceof Map ) {
                        out.add(new LinkedHashMap<String, Object>((Map)item))
                    }
                }
            }
            catch (Exception e) {
                log.debug("[NOMAD] Unable to parse lifecycle transfer manifest: ${e.message ?: e}")
            }
        }
        return out
    }

    protected void enforceLegalTransferPolicy() {
        if( !legalTransferEnabled ) {
            return
        }
        final List<Map<String, Object>> transfers = collectTransferEntries()
        if( !transfers ) {
            return
        }
        try {
            final URL url = new URL(policyServiceUrl)
            final HttpURLConnection conn = (HttpURLConnection)url.openConnection()
            conn.setRequestMethod('POST')
            conn.setRequestProperty('Content-Type', 'application/json')
            conn.setConnectTimeout(policyTimeoutMillis)
            conn.setReadTimeout(policyTimeoutMillis)
            conn.setDoOutput(true)

            final Map payload = [
                    user    : [sub: System.getenv('USER') ?: 'nextflow', groups: []],
                    job     : [id: task?.name ?: 'unknown', namespace: readMap(readMap(sessionConfig, 'nomad'), 'jobs').get('namespace') ?: 'default'],
                    transfers: transfers,
                    options : [legal_transfer_enabled: true]
            ]
            byte[] bytes = JsonOutput.toJson(payload).getBytes('UTF-8')
            conn.outputStream.withCloseable { it.write(bytes) }
            int status = conn.responseCode
            String body = null
            try {
                body = conn.inputStream?.getText('UTF-8')
            }
            catch (Exception ignored) {
                body = conn.errorStream?.getText('UTF-8')
            }
            if( status >= 400 ) {
                throw new RuntimeException("policy service returned HTTP ${status}: ${body ?: 'no body'}")
            }
            Map parsed = body ? (Map)new JsonSlurper().parseText(body) : Collections.emptyMap()
            boolean allowed = toBoolean(parsed?.allowed)
            if( !allowed ) {
                String reason = parsed?.reason?.toString() ?: parsed?.message?.toString() ?: 'transfer rejected by policy service'
                writeRemotePolicyViolation(reason, [response: parsed])
                throw new ProcessSubmitException("[NOMAD] Transfer policy violation: ${reason}")
            }
        }
        catch (ProcessSubmitException e) {
            throw e
        }
        catch (Exception e) {
            if( policyFailOpen ) {
                log.warn("[NOMAD] Transfer policy check failed with failOpen=true; continuing. Cause: ${e.message ?: e}")
                return
            }
            throw new ProcessSubmitException("[NOMAD] Transfer policy check failed: ${e.message ?: e}", e)
        }
    }
}

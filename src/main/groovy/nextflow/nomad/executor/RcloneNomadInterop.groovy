package nextflow.nomad.executor

import groovy.util.logging.Slf4j
import nextflow.exception.ProcessSubmitException
import nextflow.processor.TaskRun
import nextflow.util.Duration

import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.Properties

@Slf4j
class RcloneNomadInterop {

    private static final long DEFAULT_COMPLETION_TIMEOUT_MS = 60_000L
    private static final String DEFAULT_CONFIG_DELIVERY = 'hostPath'
    private static final String DEFAULT_SYNC_BACK = 'all'
    private static final String RUNTIME_METADATA_FILE = '.nf-rclone/runtime.properties'

    private final TaskRun task
    private final Map sessionConfig
    private final Path sessionWorkDir

    final boolean enabled
    final String remote
    final String remotePath
    final String configDelivery
    final String syncBack
    final long completionTimeoutMillis
    final String rcloneConfigPath

    private List<String> submitCommand = Collections.emptyList()
    private Map<String, String> submitEnv = Collections.emptyMap()

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
        this.completionTimeoutMillis = parseDurationMillis(workScope.get('completionTimeout'), DEFAULT_COMPLETION_TIMEOUT_MS)
        this.rcloneConfigPath = resolveRcloneConfigPath(sessionWorkDir, rcloneScope)
    }

    List<String> getSubmitCommand() {
        return submitCommand
    }

    Map<String, String> getSubmitEnv() {
        return submitEnv
    }

    String getRemoteExitLocation() {
        return remoteExitFile()
    }

    Object createCopyStrategy(TaskRun taskRun) {
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
            catch (NoSuchMethodException ignored) {
                return strategyClass
                        .getConstructor(rcloneConfigClass, String)
                        .newInstance(rcloneConfig, rcloneConfigPath)
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

    void prepare() {
        if( !enabled ) {
            return
        }
        validateConfiguration()
        uploadCommandFiles()
        buildBootstrapSubmission()
    }

    Integer synchronizeCompletion() {
        if( !enabled ) {
            return null
        }
        validateConfiguration()

        Integer remoteExit = awaitRemoteExitCode()
        if( syncBack == 'none' ) {
            copyCommandMetadata()
        }
        else {
            copyAllArtifacts()
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
        if( !rcloneConfigPath ) {
            throw new ProcessSubmitException('[NOMAD] nf-rclone interop enabled but rclone config path could not be resolved')
        }
        if( !Files.exists(Path.of(rcloneConfigPath)) ) {
            throw new ProcessSubmitException("[NOMAD] nf-rclone interop could not find rclone config at `${rcloneConfigPath}`")
        }
    }

    protected void uploadCommandFiles() {
        final List<String> cmd = [
                'rclone', 'copy',
                '--config', rcloneConfigPath,
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
        final Map<String, String> env = new LinkedHashMap<>()
        env.put('NXF_RCLONE_REMOTE_WORKDIR', remoteTaskDir())

        if( configDelivery == 'inline' ) {
            String encoded = Base64.encoder.encodeToString(Files.readAllBytes(Path.of(rcloneConfigPath)))
            env.put('NXF_RCLONE_CONFIG_B64', encoded)
        }
        else {
            env.put('NXF_RCLONE_CONFIG', rcloneConfigPath)
        }

        submitEnv = env
        submitCommand = ['bash', '-c', bootstrapScript()]
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
        return "${remote}:${base}${taskHash()}/"
    }

    protected String remoteExitFile() {
        return "${remoteTaskDir()}.exitcode"
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

    protected static String bootstrapScript() {
        return '''\
set -euo pipefail
TASK_DIR="${NOMAD_TASK_DIR:-$PWD}/nf-rclone-task"
mkdir -p "$TASK_DIR"
cd "$TASK_DIR"

if [ -n "${NXF_RCLONE_CONFIG_B64:-}" ]; then
  if printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode >/dev/null 2>&1; then
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 --decode > .rclone.conf
  else
    printf '%s' "$NXF_RCLONE_CONFIG_B64" | base64 -d > .rclone.conf
  fi
  chmod 600 .rclone.conf || true
  export NXF_RCLONE_CONFIG="$PWD/.rclone.conf"
fi

if [ -z "${NXF_RCLONE_CONFIG:-}" ]; then
  echo "[NOMAD] Missing NXF_RCLONE_CONFIG for nf-rclone bootstrap" >&2
  exit 127
fi

rclone copy --config "$NXF_RCLONE_CONFIG" --include '.command.*' "${NXF_RCLONE_REMOTE_WORKDIR}/" ./
set +e
bash .command.run
_exit_code=$?
set -e
printf '%s' "$_exit_code" > .exitcode
rclone copy --config "$NXF_RCLONE_CONFIG" ./ "${NXF_RCLONE_REMOTE_WORKDIR}/"
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
}

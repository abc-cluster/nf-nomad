package nextflow.nomad.executor

import nextflow.executor.Executor
import nextflow.nomad.config.NomadConfig
import nextflow.processor.TaskBean
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Timeout

import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.UUID

@Timeout(420)
@Stepwise
@Requires({
    System.getenv('NF_NOMAD_TEST_ENV') == 'local' &&
            System.getenv('NF_NOMAD_RCLONE_CONF') &&
            new File(System.getenv('NF_NOMAD_RCLONE_CONF')).canRead() &&
            System.getenv('NF_NOMAD_RCLONE_REMOTE') &&
            System.getenv('NF_NOMAD_RCLONE_REMOTE_PATH_PREFIX') &&
            System.getenv('NF_NOMAD_RCLONE_TEST_IMAGE')
})
class LocalRcloneRemoteWorkdirIntegrationSpec extends Specification {

    @Shared NomadConfig config
    @Shared NomadService service
    @Shared Path sessionWorkDir
    @Shared Path localWorkRoot
    @Shared List<String> submittedJobIds = []
    @Shared String rcloneConfPath
    @Shared String rcloneRemote
    @Shared String rclonePathPrefix
    @Shared String containerImage
    @Shared String sidecarDriver
    @Shared String sidecarImage
    @Shared String sidecarUser
    @Shared String runPrefix
    @Shared boolean keepDebugArtifacts = Boolean.valueOf(System.getenv('NF_NOMAD_KEEP_DEBUG_ARTIFACTS') ?: 'false')

    def setupSpec() {
        def addr = System.getenv('NOMAD_ADDR') ?: 'http://localhost:4646'
        def dc = System.getenv('NOMAD_DC') ?: null
        this.rcloneConfPath = System.getenv('NF_NOMAD_RCLONE_CONF')
        this.rcloneRemote = System.getenv('NF_NOMAD_RCLONE_REMOTE')
        this.rclonePathPrefix = System.getenv('NF_NOMAD_RCLONE_REMOTE_PATH_PREFIX')
        this.containerImage = System.getenv('NF_NOMAD_RCLONE_TEST_IMAGE')
        this.sidecarDriver = System.getenv('NF_NOMAD_RCLONE_SIDECAR_DRIVER')
        this.sidecarImage = System.getenv('NF_NOMAD_RCLONE_SIDECAR_IMAGE')
        this.sidecarUser = System.getenv('NF_NOMAD_RCLONE_SIDECAR_USER')
        this.runPrefix = normalizePath("${rclonePathPrefix}/nf-nomad-rclone-it/${UUID.randomUUID()}/")

        def clientOpts = [address: addr]
        def jobsOpts = [deleteOnCompletion: false, cleanup: 'never']
        if (dc) jobsOpts.datacenters = dc

        config = new NomadConfig(client: clientOpts, jobs: jobsOpts)
        service = new NomadService(config)
        sessionWorkDir = Files.createTempDirectory('nf-nomad-rclone-session')
        localWorkRoot = sessionWorkDir
        writeRuntimeMetadata(sessionWorkDir, rcloneConfPath)
    }

    def cleanupSpec() {
        if (!keepDebugArtifacts) {
            submittedJobIds.each { jobId ->
                try { service.jobPurge(jobId) } catch (ignored) {}
            }
        }
        if (!keepDebugArtifacts) {
            try {
                runCommand(['rclone', 'purge', '--config', rcloneConfPath, "${rcloneRemote}:${runPrefix}"])
            } catch (ignored) {}
        }
        service?.close()
        if (!keepDebugArtifacts) {
            localWorkRoot?.toFile()?.deleteDir()
            sessionWorkDir?.toFile()?.deleteDir()
        }
    }

    private static TaskBean taskBean(String taskHash, Path workDir, String outputName) {
        def bean = new TaskBean()
        bean.name = "rclone-interop-${taskHash}"
        bean.workDir = workDir
        bean.targetDir = workDir
        bean.script = "echo '${taskHash}' > ${outputName}"
        bean.shell = ['bash']
        bean.inputFiles = [:]
        bean.outputFiles = [outputName]
        return bean
    }

    void 'should execute remote-workdir bootstrap flow with hostPath config delivery'() {
        given:
        def taskHash = UUID.randomUUID().toString().replace('-', '')
        def workDir = createTaskWorkDir(taskHash)
        writeCommandFiles(workDir, "result-hostpath.txt", "hostpath")
        def task = mockTask(workDir, taskHash, "result-hostpath.txt")
        def sessionConfig = rcloneSessionConfig('hostPath')
        def handler = new NomadTaskHandler(task, config, service, sessionConfig, sessionWorkDir)

        when:
        handler.submitTask()
        submittedJobIds.add(readHandlerJobName(handler))
        def completed = awaitCompletion(handler, 150)

        then:
        completed
        Files.exists(workDir.resolve(TaskRun.CMD_EXIT))
        Files.readString(workDir.resolve(TaskRun.CMD_EXIT)).trim() == '0'
        Files.exists(workDir.resolve('result-hostpath.txt'))
        remoteExitCode(workDir) == '0'
    }

    void 'should execute remote-workdir bootstrap flow with inline config delivery'() {
        given:
        def taskHash = UUID.randomUUID().toString().replace('-', '')
        def workDir = createTaskWorkDir(taskHash)
        writeCommandFiles(workDir, "result-inline.txt", "inline")
        def task = mockTask(workDir, taskHash, "result-inline.txt")
        def sessionConfig = rcloneSessionConfig('inline')
        def handler = new NomadTaskHandler(task, config, service, sessionConfig, sessionWorkDir)

        when:
        handler.submitTask()
        submittedJobIds.add(readHandlerJobName(handler))
        def completed = awaitCompletion(handler, 150)

        then:
        completed
        Files.exists(workDir.resolve(TaskRun.CMD_EXIT))
        Files.readString(workDir.resolve(TaskRun.CMD_EXIT)).trim() == '0'
        Files.exists(workDir.resolve('result-inline.txt'))
        remoteExitCode(workDir) == '0'
    }

    private Map<String, Object> rcloneSessionConfig(String configDelivery) {
        return [
                rclone: [
                        rcloneConf  : rcloneConfPath,
                        rcloneWorkDir: [
                                enabled          : true,
                                remote           : rcloneRemote,
                                remotePath       : runPrefix,
                                transferMode     : 'sidecar',
                                sidecarDriver    : sidecarDriver,
                                sidecarImage     : sidecarImage,
                                sidecarUser      : sidecarUser,
                                configDelivery   : configDelivery,
                                completionTimeout: '120s',
                                syncBack         : 'all'
                        ]
                ]
        ] as Map<String, Object>
    }

    private TaskRun mockTask(Path workDir, String taskHash, String outputName) {
        def bean = taskBean(taskHash, workDir, outputName)
        return Mock(TaskRun) {
            getName() >> "rclone-interop-${taskHash}"
            getContainer() >> containerImage
            getShell() >> ['bash']
            getWorkDir() >> workDir
            getWorkDirStr() >> workDir.toString()
            getTargetDir() >> workDir
            getConfig() >> Mock(TaskConfig) {
                getStageInMode() >> null
                getStageOutMode() >> null
            }
            getProcessor() >> Mock(TaskProcessor) {
                getExecutor() >> Mock(Executor) {
                    isFusionEnabled() >> false
                }
            }
            toTaskBean() >> bean
        }
    }

    private void writeCommandFiles(Path workDir, String outputName, String marker) {
        Files.writeString(workDir.resolve(TaskRun.CMD_INFILE), "echo '${marker}' > ${outputName}\n")
        Files.writeString(workDir.resolve('.command.sh'), "#!/usr/bin/env bash\nset -euo pipefail\necho '${marker}' > .command.out\n")
        Files.writeString(workDir.resolve('.command.run'), "#!/usr/bin/env bash\nset -euo pipefail\necho '${marker}' > ${outputName}\n")
        workDir.resolve('.command.sh').toFile().setExecutable(true, true)
        workDir.resolve('.command.run').toFile().setExecutable(true, true)
    }

    private boolean awaitCompletion(NomadTaskHandler handler, int maxAttempts) {
        int attempt = 0
        while (attempt < maxAttempts) {
            if (handler.checkIfCompleted()) {
                return true
            }
            sleep(1000)
            attempt++
        }
        return false
    }

    private Path createTaskWorkDir(String taskHash) {
        def prefix = taskHash.substring(0, 2)
        def suffix = taskHash.substring(2)
        return Files.createDirectories(localWorkRoot.resolve(prefix).resolve(suffix))
    }

    private String remoteExitCode(Path taskWorkDir) {
        def relative = localWorkRoot.relativize(taskWorkDir).toString().replace('\\', '/')
        def remoteExit = "${rcloneRemote}:${normalizePath(runPrefix)}${relative}/.exitcode"
        def result = runCommand(['rclone', 'cat', '--config', rcloneConfPath, remoteExit])
        return result.exitCode == 0 ? result.stdout.trim() : null
    }

    private static String readHandlerJobName(NomadTaskHandler handler) {
        def f = NomadTaskHandler.class.getDeclaredField('jobName')
        f.setAccessible(true)
        return (String)f.get(handler)
    }

    private static String normalizePath(String value) {
        if (!value) return ''
        return value.endsWith('/') ? value : "${value}/"
    }

    private static CommandResult runCommand(List<String> command) {
        Process proc = command.execute()
        StringBuffer stdout = new StringBuffer()
        StringBuffer stderr = new StringBuffer()
        proc.consumeProcessOutput(stdout, stderr)
        proc.waitFor()
        return new CommandResult(proc.exitValue(), stdout.toString(), stderr.toString())
    }

    private static void writeRuntimeMetadata(Path sessionDir, String confPath) {
        Path metaDir = sessionDir.resolve('.nf-rclone')
        Files.createDirectories(metaDir)
        Properties props = new Properties()
        props.setProperty('version', '1')
        props.setProperty('rclone.configPath', confPath)
        Files.newOutputStream(metaDir.resolve('runtime.properties')).withCloseable { out ->
            props.store(out, 'nf-rclone runtime metadata for integration tests')
        }
    }

    private static class CommandResult {
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

package nextflow.nomad.executor

import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties

class RcloneNomadInteropSpec extends Specification {

    @TempDir
    Path tempDir

    void 'should keep interop disabled when rcloneWorkDir is not enabled'() {
        given:
        def task = mockTask(tempDir.resolve('work').resolve('hash-1'))
        def cfg = [rclone: [rcloneWorkDir: [enabled: false]]]

        when:
        def interop = new RcloneNomadInterop(task, cfg, tempDir.resolve('session'))

        then:
        !interop.enabled
    }

    void 'should prepare bootstrap submit command and upload task scripts'() {
        given:
        def sessionDir = tempDir.resolve('session')
        def workDir = tempDir.resolve('work').resolve('ab12cd34')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\n')

        def rcloneConf = writeRuntimeMetadata(sessionDir)
        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run', configDelivery: 'inline']]]

        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        interop.enabled
        interop.submitCommand[0] == 'bash'
        interop.submitCommand[1] == '-c'
        interop.submitCommand[2].contains('NXF_RCLONE_REMOTE_WORKDIR')
        interop.submitEnv.get('NXF_RCLONE_REMOTE_WORKDIR') == 'minio:work/run/ab12cd34/'
        interop.submitEnv.containsKey('NXF_RCLONE_CONFIG_B64')
        interop.commands.any { List<String> cmd ->
            cmd[0] == 'rclone' && cmd.contains('.command.*') && cmd.contains(workDir.toString())
        }

        and:
        rcloneConf != null
    }

    void 'should synchronize completion and materialize local exitcode'() {
        given:
        def sessionDir = tempDir.resolve('session')
        def workDir = tempDir.resolve('work').resolve('ef56gh78')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run', completionTimeout: '2s']]]
        def interop = new TestInterop(task, cfg, sessionDir)
        interop.prepare()

        when:
        def code = interop.synchronizeCompletion()

        then:
        code == 17
        Files.readString(workDir.resolve(TaskRun.CMD_EXIT)).trim() == '17'
        interop.commands.any { List<String> cmd -> cmd[0] == 'rclone' && cmd[1] == 'cat' }
        interop.commands.any { List<String> cmd -> cmd[0] == 'rclone' && cmd[1] == 'copy' && cmd.contains(workDir.toString()) }
    }

    void 'should return null when remote exitcode is not available before timeout'() {
        given:
        def sessionDir = tempDir.resolve('session-timeout')
        def workDir = tempDir.resolve('work').resolve('zz99yy88')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run', completionTimeout: '1s']]]
        def interop = new TimeoutInterop(task, cfg, sessionDir)
        interop.prepare()

        when:
        def code = interop.synchronizeCompletion()

        then:
        code == null
        !Files.exists(workDir.resolve(TaskRun.CMD_EXIT))
        interop.commands.count { List<String> cmd -> cmd[0] == 'rclone' && cmd[1] == 'cat' } >= 1
    }

    void 'should gracefully return null copy strategy when nf-rclone classes are unavailable'() {
        given:
        def sessionDir = tempDir.resolve('session-copy-strategy')
        def workDir = tempDir.resolve('work').resolve('xy12zz34')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run']]]
        def interop = new RcloneNomadInterop(task, cfg, sessionDir)

        when:
        def strategy = interop.createCopyStrategy(task)

        then:
        strategy == null
    }

    private TaskRun mockTask(Path workDir) {
        Files.createDirectories(workDir)
        return Mock(TaskRun) {
            getName() >> 'nf-rclone-task'
            getWorkDir() >> workDir
            getHash() >> 'ab12cd34'
        }
    }

    private Path writeRuntimeMetadata(Path sessionDir) {
        Files.createDirectories(sessionDir.resolve('.nf-rclone'))
        Path conf = sessionDir.resolve('.nf-rclone').resolve('rclone-nf.conf')
        Files.writeString(conf, '[minio]\ntype = s3\n')
        Properties props = new Properties()
        props.setProperty('rclone.configPath', conf.toString())
        Files.newOutputStream(sessionDir.resolve('.nf-rclone').resolve('runtime.properties')).withCloseable { out ->
            props.store(out, 'test')
        }
        return conf
    }

    private static class TestInterop extends RcloneNomadInterop {
        final List<List<String>> commands = []

        TestInterop(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
            super(task, sessionConfig, sessionWorkDir)
        }

        @Override
        protected CommandResult runCommand(List<String> command) {
            commands << command
            if( command.size() > 1 && command[1] == 'cat' ) {
                return new CommandResult(0, '17\n', '')
            }
            return new CommandResult(0, '', '')
        }
    }

    private static class TimeoutInterop extends RcloneNomadInterop {
        final List<List<String>> commands = []

        TimeoutInterop(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
            super(task, sessionConfig, sessionWorkDir)
        }

        @Override
        protected CommandResult runCommand(List<String> command) {
            commands << command
            if( command.size() > 1 && command[1] == 'cat' ) {
                return new CommandResult(1, '', 'not found')
            }
            return new CommandResult(0, '', '')
        }
    }
}

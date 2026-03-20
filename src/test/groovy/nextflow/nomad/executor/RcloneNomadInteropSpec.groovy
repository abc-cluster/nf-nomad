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
        def workDir = sessionDir.resolve('ab').resolve('12cd34')
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
        interop.submitEnv.get('NXF_RCLONE_REMOTE_WORKDIR') == 'minio:work/run/ab/12cd34/'
        interop.submitEnv.containsKey('NXF_RCLONE_CONFIG_B64')
        interop.commands.any { List<String> cmd ->
            cmd[0] == 'rclone' && cmd.contains('.command.*') && cmd.contains(workDir.toString())
        }

        and:
        rcloneConf != null
    }

    void 'should preserve nextflow NN/HASH layout in remote task path when under session workdir'() {
        given:
        def sessionDir = tempDir.resolve('session-work')
        def workDir = sessionDir.resolve('97').resolve('f9c8ee6cf00be6c1dd40e045c39640')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run', configDelivery: 'inline']]]
        def interop = new TestInterop(task, cfg, sessionDir)
        def expectedRemote = 'minio:work/run/97/f9c8ee6cf00be6c1dd40e045c39640/'

        when:
        interop.prepare()

        then:
        interop.submitEnv.get('NXF_RCLONE_REMOTE_WORKDIR') == expectedRemote
        interop.commands.any { List<String> cmd -> cmd.contains(expectedRemote) }
    }

    void 'should synchronize completion and materialize local exitcode'() {
        given:
        def sessionDir = tempDir.resolve('session')
        def workDir = sessionDir.resolve('ef').resolve('56ab78')
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

    void 'should prepare lifecycle sidecar tasks when transferMode is sidecar'() {
        given:
        def sessionDir = tempDir.resolve('session-sidecar')
        def workDir = sessionDir.resolve('9a').resolve('bc12ab34')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run', transferMode: 'sidecar', configDelivery: 'inline', sidecarUser: 'nfx']]]
        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        interop.submitCommand[0] == 'bash'
        interop.submitCommand[1] == '-lc'
        !interop.submitCommand[2].contains('rclone copy --config')
        interop.submitEnv == [:]
        !interop.submitEnv.containsKey('NXF_RCLONE_CONFIG')
        !interop.submitEnv.containsKey('NXF_RCLONE_CONFIG_B64')
        interop.lifecycleTasks.size() == 2
        interop.lifecycleTasks*.hook == ['prestart', 'poststop']
        interop.lifecycleTasks*.driver.unique() == ['raw_exec']
        interop.lifecycleTasks*.user.unique() == ['nfx']
        interop.lifecycleTasks.every { it.env.containsKey('NXF_RCLONE_REMOTE_WORKDIR') }
        interop.lifecycleTasks.every { it.env.containsKey('NXF_RCLONE_CONFIG_B64') }
    }

    void 'should prepare docker lifecycle sidecars when sidecarDriver is docker'() {
        given:
        def sessionDir = tempDir.resolve('session-sidecar-docker')
        def workDir = sessionDir.resolve('9b').resolve('cd12ab34')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\\n')
        def rcloneConf = writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [
                enabled      : true,
                remote       : 'minio',
                remotePath   : 'work/run',
                transferMode : 'sidecar',
                sidecarDriver: 'docker',
                sidecarImage : 'rclone/rclone:latest'
        ]]]
        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        interop.lifecycleTasks*.driver.unique() == ['docker']
        interop.lifecycleTasks.every { it.user == null }
        interop.lifecycleTasks.every { it.config.get('image') == 'rclone/rclone:latest' }
        interop.lifecycleTasks.every { it.config.get('entrypoint') == ['sh'] }
        interop.lifecycleTasks.every { it.config.get('network_mode') == 'host' }
        interop.lifecycleTasks.every { it.config.get('volumes') == ["${rcloneConf}:${rcloneConf}:ro"] }
        interop.lifecycleTasks.every { ((List)it.config.get('volumes'))[0] instanceof String }
        interop.lifecycleTasks.every { it.command.size() == 2 }
        interop.lifecycleTasks.every { it.command[0] == '-lc' }
        interop.lifecycleTasks.every { it.command[1].contains('set -eu') }
    }

    void 'should reject docker lifecycle sidecars without sidecarImage'() {
        given:
        def sessionDir = tempDir.resolve('session-sidecar-docker-missing-image')
        def workDir = sessionDir.resolve('9c').resolve('de12ab34')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [
                enabled      : true,
                remote       : 'minio',
                remotePath   : 'work/run',
                transferMode : 'sidecar',
                sidecarDriver: 'docker'
        ]]]
        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        def e = thrown(nextflow.exception.ProcessSubmitException)
        e.message.contains('sidecarImage')
    }

    void 'should use hostPath config delivery by default'() {
        given:
        def sessionDir = tempDir.resolve('session-hostpath')
        def workDir = sessionDir.resolve('a1').resolve('b2ab34cd')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\n')
        def rcloneConf = writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run']]]
        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        interop.submitEnv.get('NXF_RCLONE_CONFIG') == rcloneConf.toString()
        !interop.submitEnv.containsKey('NXF_RCLONE_CONFIG_B64')
    }

    void 'should use hostPath config delivery by default in sidecar mode'() {
        given:
        def sessionDir = tempDir.resolve('session-hostpath-sidecar')
        def workDir = sessionDir.resolve('a2').resolve('b3cd45ef')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\\n')
        def rcloneConf = writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [
                enabled     : true,
                remote      : 'minio',
                remotePath  : 'work/run',
                transferMode: 'sidecar'
        ]]]
        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        interop.configDelivery == 'hostPath'
        interop.lifecycleTasks.size() == 2
        interop.lifecycleTasks.every { it.env.get('NXF_RCLONE_CONFIG') == rcloneConf.toString() }
        interop.lifecycleTasks.every { !it.env.containsKey('NXF_RCLONE_CONFIG_B64') }
    }

    void 'should return null when remote exitcode is not available before timeout'() {
        given:
        def sessionDir = tempDir.resolve('session-timeout')
        def workDir = sessionDir.resolve('b1').resolve('c2d3e4f5')
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
        def workDir = sessionDir.resolve('c1').resolve('d2e3f4a5')
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

    void 'should use metadata-only copy command when syncBack is none'() {
        given:
        def sessionDir = tempDir.resolve('session-none')
        def workDir = sessionDir.resolve('d1').resolve('e2f3a4b5')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run', syncBack: 'none', completionTimeout: '2s']]]
        def interop = new TestInterop(task, cfg, sessionDir)
        interop.prepare()

        when:
        def code = interop.synchronizeCompletion()

        then:
        code == 17
        interop.commands.any { List<String> cmd ->
            cmd[0] == 'rclone' &&
                    cmd[1] == 'copy' &&
                    cmd.contains('--include') &&
                    cmd.contains('.exitcode') &&
                    cmd.contains('.command.*')
        }
    }

    void 'should fail fast when task workdir is outside session workdir'() {
        given:
        def sessionDir = tempDir.resolve('session-strict')
        Files.createDirectories(sessionDir)
        def workDir = tempDir.resolve('outside').resolve('ab').resolve('12cd34')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.run'), 'echo ok\n')
        Files.writeString(workDir.resolve('.command.sh'), 'echo ok\n')
        writeRuntimeMetadata(sessionDir)

        def task = mockTask(workDir)
        def cfg = [rclone: [rcloneWorkDir: [enabled: true, remote: 'minio', remotePath: 'work/run']]]
        def interop = new TestInterop(task, cfg, sessionDir)

        when:
        interop.prepare()

        then:
        def e = thrown(nextflow.exception.ProcessSubmitException)
        e.message.contains('nested under session workDir')
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

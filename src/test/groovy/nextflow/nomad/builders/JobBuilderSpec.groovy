/*
 * Copyright 2023-, Stellenbosch University, South Africa
 * Copyright 2024, Evaluacion y Desarrollo de Negocios, Spain
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.nomad.builders


import io.nomadproject.client.model.Task
import nextflow.executor.Executor
import nextflow.executor.ExecutorConfig
import nextflow.nomad.config.NomadJobOpts
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import spock.lang.Specification

/**
 * Unit test for Nomad JobBuilder
 *
 * @author : Abhinav Sharma <abhi18av@outlook.com>
 */


class JobBuilderSpec extends Specification {

    def "test JobBuilder withId method"() {
        given:
        def jobBuilder = new JobBuilder()

        when:
        def jb = jobBuilder
                .withId("test-id")
                .build()

        then:
        jb.ID == "test-id"
    }


    def "test createTask method"() {
        given:
        def jobOpts = Mock(NomadJobOpts) {
            driver >> "docker"
        }
        def taskRun = Mock(TaskRun)
        def args = ["arg1", "arg2"]
        def env = ["key": "value"]

        taskRun.container >> "test-container"
        taskRun.workDir >> new File("/test/workdir").toPath()
        taskRun.getConfig() >> [cpus: 2, memory: "1GB"]

        when:
        def task = JobBuilder.createTask(taskRun, args, env, jobOpts)

        then:
        task.name == "nf-task"
        task.driver == "docker"
        task.config.image == "test-container"
        task.config.command == "arg1"
        task.config.args == ["arg2"]
        task.env == env
        task.resources.cores == 2
        task.resources.memoryMB == 1024
    }


    def "test createTaskGroup method"() {
        given:
        def volumes = [{ type "csi" path "/container/path"}]

        def jobOpts = Mock(NomadJobOpts)

        def taskRun = Mock(TaskRun)
        def args = ["arg1", "arg2"]
        def env = ["key": "value"]

        taskRun.container >> "test-container"
        taskRun.workDir >> new File("/test/workdir").toPath()
        taskRun.getConfig() >> [cpus: 2, memory: "1GB"]

        when:
        def taskGroup = JobBuilder.createTaskGroup(taskRun, args, env, jobOpts)

        then:
        taskGroup.name == "nf-taskgroup"
        taskGroup.tasks.size() == 1
        taskGroup.tasks[0].name == "nf-task"
        taskGroup.tasks[0].config.image == "test-container"
        taskGroup.tasks[0].config.command == "arg1"
        taskGroup.tasks[0].config.args == ["arg2"]
        taskGroup.tasks[0].env == env
    }


    def "test createTask with pbs driver produces hpc config"() {
        given:
        def jobOpts = Mock(NomadJobOpts) {
            driver >> "pbs"
        }
        def mockTaskConfig = new TaskConfig([
                queue: 'gpu',
                time: '4h',
                cpus: 8,
                memory: '16 GB',
                clusterOptions: '-l ngpus=2',
        ])
        def mockExecConfig = Mock(ExecutorConfig) {
            getExecConfigProp('nomad', 'account', null) >> "myaccount"
        }
        def mockExecutor = Mock(Executor) {
            getConfig() >> mockExecConfig
        }
        def mockProcessor = Mock(TaskProcessor) {
            getExecutor() >> mockExecutor
        }
        def taskRun = Mock(TaskRun) {
            workDir >> new File("/scratch/work/ab/cd1234").toPath()
            getConfig() >> mockTaskConfig
            processor >> mockProcessor
        }
        def args = ["bash", ".command.run"]
        def env = ["NF_TASK_NAME": "hello"]

        when:
        def task = JobBuilder.createTask(taskRun, args, env, jobOpts)

        then:
        task.name == "nf-task"
        task.driver == "pbs"
        task.config.command == "bash"
        task.config.args == [".command.run"]
        task.config.work_dir == "/scratch/work/ab/cd1234"
        task.config.stdout_file == "/scratch/work/ab/cd1234/.command.log"
        task.config.stderr_file == "/scratch/work/ab/cd1234/.command.log"
        task.config.queue == "gpu"
        task.config.walltime == "04:00:00"
        task.config.cpus_per_task == 8
        task.config.memory == 16384
        task.config.account == "myaccount"
        task.config.extra_args == ["-l", "ngpus=2"]
        // Docker-specific fields should NOT be present
        task.config.image == null
        task.config.privileged == null
        task.env == env
    }


    def "test createLifecycleTask creates prestart task"() {
        when:
        def task = JobBuilder.createLifecycleTask(
                "stage-in",
                JobBuilder.LIFECYCLE_HOOK_PRESTART,
                "raw_exec",
                [command: "/usr/bin/rclone", args: ["copy", "s3://bucket/input", "/scratch/work"]],
                ["RCLONE_CONFIG": "/etc/rclone.conf"],
        )

        then:
        task.name == "stage-in"
        task.driver == "raw_exec"
        task.config.command == "/usr/bin/rclone"
        task.config.args == ["copy", "s3://bucket/input", "/scratch/work"]
        task.env["RCLONE_CONFIG"] == "/etc/rclone.conf"
        task.lifecycle.hook == "prestart"
        task.lifecycle.sidecar == false
    }


    def "test createLifecycleTask creates poststop task"() {
        when:
        def task = JobBuilder.createLifecycleTask(
                "stage-out",
                JobBuilder.LIFECYCLE_HOOK_POSTSTOP,
                "raw_exec",
                [command: "/usr/bin/rclone", args: ["copy", "/scratch/work/output", "s3://bucket/output"]],
        )

        then:
        task.name == "stage-out"
        task.driver == "raw_exec"
        task.lifecycle.hook == "poststop"
        task.lifecycle.sidecar == false
    }


    def "test createLifecycleTask rejects invalid hook"() {
        when:
        JobBuilder.createLifecycleTask(
                "bad-task",
                "invalid_hook",
                "raw_exec",
                [command: "echo", args: ["hello"]],
        )

        then:
        thrown(IllegalArgumentException)
    }


    def "test createLifecycleTask supports sidecar mode"() {
        when:
        def task = JobBuilder.createLifecycleTask(
                "log-shipper",
                JobBuilder.LIFECYCLE_HOOK_PRESTART,
                "raw_exec",
                [command: "/usr/bin/tail", args: ["-f", "/var/log/app.log"]],
                [:],
                true,
        )

        then:
        task.lifecycle.hook == "prestart"
        task.lifecycle.sidecar == true
    }


    def "test createTaskGroup includes lifecycle tasks"() {
        given:
        def jobOpts = Mock(NomadJobOpts) {
            driver >> "pbs"
        }
        def taskRun = Mock(TaskRun)
        taskRun.container >> null
        taskRun.workDir >> new File("/scratch/work/ab/cd1234").toPath()
        taskRun.getConfig() >> [cpus: 2, memory: "1GB"]

        def prestartTask = JobBuilder.createLifecycleTask(
                "stage-in",
                JobBuilder.LIFECYCLE_HOOK_PRESTART,
                "raw_exec",
                [command: "rclone", args: ["copy", "s3://bucket/in", "/scratch/work"]],
        )
        def poststopTask = JobBuilder.createLifecycleTask(
                "stage-out",
                JobBuilder.LIFECYCLE_HOOK_POSTSTOP,
                "raw_exec",
                [command: "rclone", args: ["copy", "/scratch/work/out", "s3://bucket/out"]],
        )

        when:
        def taskGroup = JobBuilder.createTaskGroup(
                taskRun, ["bash", ".command.run"], ["KEY": "val"], jobOpts,
                [prestartTask, poststopTask] as List<Task>,
        )

        then:
        taskGroup.name == "nf-taskgroup"
        taskGroup.tasks.size() == 3

        // Lifecycle tasks come first, main task last
        taskGroup.tasks[0].name == "stage-in"
        taskGroup.tasks[0].driver == "raw_exec"
        taskGroup.tasks[0].lifecycle.hook == "prestart"

        taskGroup.tasks[1].name == "stage-out"
        taskGroup.tasks[1].driver == "raw_exec"
        taskGroup.tasks[1].lifecycle.hook == "poststop"

        // Main task
        taskGroup.tasks[2].name == "nf-task"
        taskGroup.tasks[2].driver == "pbs"
        taskGroup.tasks[2].lifecycle == null
    }


    def "test createTaskGroup without lifecycle tasks has single main task"() {
        given:
        def jobOpts = Mock(NomadJobOpts)
        def taskRun = Mock(TaskRun)
        taskRun.container >> "ubuntu"
        taskRun.workDir >> new File("/test/workdir").toPath()
        taskRun.getConfig() >> [cpus: 1, memory: "512MB"]

        when:
        def taskGroup = JobBuilder.createTaskGroup(taskRun, ["bash", "-c", "echo"], ["K": "V"], jobOpts)

        then:
        taskGroup.tasks.size() == 1
        taskGroup.tasks[0].name == "nf-task"
        taskGroup.tasks[0].lifecycle == null
    }

}

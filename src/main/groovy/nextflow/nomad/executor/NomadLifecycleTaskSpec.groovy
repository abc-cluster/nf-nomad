package nextflow.nomad.executor

import groovy.transform.CompileStatic

@CompileStatic
class NomadLifecycleTaskSpec {
    String name
    String hook
    boolean sidecar = false
    String driver = 'raw_exec'
    String user
    List<String> command = Collections.emptyList()
    Map<String, Object> config = Collections.emptyMap()
    Map<String, String> env = Collections.emptyMap()
    Integer cpu = 200
    Integer memoryMb = 128
}

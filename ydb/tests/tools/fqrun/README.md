# FQ run tool

Tool can be used to execute streaming queries by using FQ proxy infrastructure.

For profiling memory allocations build fqrun with ya make flags `-D PROFILE_MEMORY_ALLOCATIONS -D CXXFLAGS=-DPROFILE_MEMORY_ALLOCATIONS`.

## Scripts

* `flame_graph.sh` - script for collecting flame graphs in svg format, usage:
    ```(bash)
    ./flame_graph.sh [graph collection time in seconds]
    ```

## Examples

### Queries

* Run select 42:
    ```(bash)
    ./fqrun -s "SELECT 42"
    ```

### Logs

* Setup log settings:
    ```(bash)
    ./fqrun -s "SELECT 42" --log-default=warn --log FQ_RUN_ACTOR=trace --log-file query.log
    ```

### Cluster

* Embedded UI:
    ```(bash)
    ./fqrun -M 32000
    ```

    Monitoring endpoint: http://localhost:32000

* gRPC endpoint:
    ```(bash)
    ./fqrun -G 32000
    ```

    Connect with ydb CLI: `ydb -e grpc://localhost:32000 -d /Root`

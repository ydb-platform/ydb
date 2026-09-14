# FQ run tool

Tool can be used to execute streaming queries by using FQ proxy infrastructure.

For profiling memory allocations build fqrun with ya make flags `-D PROFILE_MEMORY_ALLOCATIONS -D CXXFLAGS=-DPROFILE_MEMORY_ALLOCATIONS`.

## Scripts

* `flame_graph.sh` - script for collecting flame graphs in svg format, usage:
    ```(bash)
    ./flame_graph.sh [graph collection time in seconds] [use sudo]
    ```

## Examples

### Queries

* Stream query:
    ```(bash)
    ./fqrun -s "SELECT 42"
    ```

* In place analytics query:
    ```(bash)
    ./fqrun -s "SELECT 42" -C analytics
    ```

* Analytics query with remote compute database:
    1.  Start shared ydb tenant:
        ```(bash)
        ./kqprun -G 12345 --shared db
        ```

    1. Execute query:
        ```(bash)
        ./fqrun -s "SELECT 42" -C analytics --shared-compute-db /Root/db@localhost:12345
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

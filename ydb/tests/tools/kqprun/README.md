# KqpRun tool

Tool can be used to execute queries by using kikimr provider.

For profiling memory allocations build kqprun with ya make flag `-D PROFILE_MEMORY_ALLOCATIONS`.

## Examples

### Queries

* Run select 42:
    ```(bash)
    ./kqprun --sql "SELECT 42"
    ```

* Queries shooting:
    ```(bash)
    ./kqprun --sql "SELECT 42" -C async --loop-count 0 --loop-delay 100 --inflight-limit 10
    ```

### Logs

* Setup log settings (`-C query` for clear logs):
    ```(bash)
    ./kqprun --sql "SELECT 42" -C query --log-default=warn --log KQP_YQL=trace --log-file query.log
    ```

* Trace opt:
    ```(bash)
    ./kqprun --sql "SELECT 42" -C query -T script
    ```

* Runtime statistics:
    ```(bash)
    ./kqprun --sql "SELECT 42" --script-statistics stats.log --script-timeline-file timeline.svg
    ```

### Cluster

* Embedded UI:
    ```(bash)
    ./kqprun -M 32000
    ```

    Monitoring endpoint: https://localhost:32000

* gRPC endpoint:
    ```(bash)
    ./kqprun -G 32000
    ```

    Connect with ydb CLI: `ydb -e grpc://localhost:32000 -d /Root`

* Static storage:
    ```(bash)
    ./kqprun -M 32000 --storage-path ./storage --storage-size 32
    ```

* Create serverless domain and execute query in this domain:
    ```(bash)
    ./kqprun -M 32000 --shared my-shared --serverless my-serverless --sql "SELECT 42" -D /Root/my-serverless
    ```

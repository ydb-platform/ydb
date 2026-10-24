# KqpRun tool

Tool can be used to execute queries by using kikimr provider.

For profiling memory allocations build kqprun with ya make flag `-D PROFILE_MEMORY_ALLOCATIONS -D CXXFLAGS=-DPROFILE_MEMORY_ALLOCATIONS`.

## Scripts

* `flame_graph.sh` - script for collecting flame graphs in svg format, usage:
    ```(bash)
    ./scripts/flame_graph.sh [graph collection time in seconds] [use sudo]
    ```

* `start_prometheus.sh` - start prometheus web UI, can be used for counters visualisation (kqprun should be run with flag `-M <monitoring port>`), usage:
    ```(bash)
    ./scripts/start_prometheus.sh <kqprun monitoring port> <web UI port> [config path]
    ```
    Prometheus UI available on `http://localhost:<web UI port>/targets`

* `start_grafana.sh` - start grafana other existing prometheus, usage:
    ```(bash)
    ./scripts/start_grafana.sh <prometheus port> <web UI port> [additional dashboards dirs]
    ```

    Command for starting prometheus and grafana:
    ```(bash)
    ./scripts/start_prometheus.sh 32000 32001 && ./scripts/start_grafana.sh 32001 32002
    ```
    Where 32000 - kqprun monitoring port, graphana UI available on http://localhost:32002 (login: `admin`, password: `admin`)

* `start_connector.sh` - start local FQ connector, usage:
    ```(bash)
    ./scripts/start_connector.sh <connector port>
    ```

* `cleanup_docker.sh` - stop created docker containers, usage:
    ```(bash)
    ./scripts/cleanup_docker.sh [name filter]
    ```

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

### Query directories

* Run all `.sql`/`.yql` files from directories (non-recursive, lexicographic filename order, other files are ignored; an empty or missing directory is an error):
    ```(bash)
    ./kqprun --scheme-query-dir schema/ --script-query-dir queries/ -A explain --continue-after-fail
    ```

    All scheme queries run before script queries. `--scheme-query`, `--scheme-query-dir`, `--script-query`, `--script-query-dir` and `--sql` can be repeated and mixed; inputs of each kind keep command line order. Use zero-padded prefixes (`001-create.sql`, `002-alter.sql`) when order matters. Progress and errors include the source file name.

    With `--continue-after-fail` the remaining queries are still executed, but the exit status is nonzero if any synchronous query failed. A failed scheme query always stops the run.

* Raise scheme limits for root database and created tenants (protobuf text `NKikimrSubDomains::TSchemeLimits`, omitted fields keep their defaults), e. g. for large schemas:
    ```(bash)
    echo 'MaxPaths: 100000 MaxTableColumns: 1000 MaxTableIndices: 100' > limits.pb.txt
    ./kqprun --scheme-limits limits.pb.txt --scheme-query-dir schema/ --script-query-dir queries/ -A explain
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

* Embedded UI, flag `-M` means the beginning of the range of ports used for different nodes:
    ```(bash)
    ./kqprun -M 32000
    ```

    Monitoring endpoint: http://localhost:32000

* gRPC endpoint, flag `-G` means the beginning of the range of ports used for different tenants:
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

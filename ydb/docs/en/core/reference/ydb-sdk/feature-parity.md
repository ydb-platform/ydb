# Comparison of SDK features

| Feature | C\+\+ | Python | Go | Java | NodeJS | C# | Rust | PHP |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| SSL/TLS support (system certificates) | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| SSL/TLS support (custom certificates) | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| Configure/enable GRPC KeepAlive (keeping the connection alive in the background) | \+ | \+ | \+ | ? | \- | \- |  | \+ |
| Regular SLO testing on the latest code version | \+ | \+/- | \+ | \+ | \+/- | \- | \- | \- |
| Issue templates on GitHub | \- | ? | \+ | \- | \+ | \+ |  | \+ |
| **Client-side balancing** |
| Load balancer initialization through Discovery/ListEndpoints | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Disable client-side load balancing (all requests to the initial Endpoint) | \+/- | \- | \+ | \- | \- | \+ |  | \+ |
| Background Discovery/ListEndpoints (by default, once a minute) | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \- |
| Support for multiple IP addresses in the initial Endpoint DNS record, some of which may not be available (DNS load balancing) | ? | \+ | \+ | ? | \- | ? | ? | ? |
| Node pessimization on transport errors | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Forced Discovery/ListEndpoints if more than half of the nodes are pessimized | \+ | \+ | \+ | \+ | \- | \+ | \+ |
| Automatic detection of the nearest DC/availability zone by TCP pings | \- | \- | \+ | \- | \- | \- | \- |
| Automatic detection of the nearest DC/availability zone by Discovery/ListEndpoints response\* | \+ | \+ | \- | \- | \- | \- | \- |
| Uniform random selection of nodes (default) | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Load balancing across all nodes of all DCs (default) | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Load balancing across all nodes of a particular DC/availability zone (for example, “a”, “vla”) | \+ | \+ | \+ | ? | \- | \- | \- |
| Load balancing across all nodes of all local DCs | \+ | \+ | \+ | ? | \- | \- | \- |
| **Credentials providers** |
| Anonymous (default) | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Static (user - password) | \+ | \+ | \+ | \+ | \- | \- | \+ | \- |
| Token: IAM, Access token | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Service account (Yandex.Cloud specific) | \+ | \+ | \+ | \+ | \+ | \+ | \- | \+ |
| Metadata (Yandex.Cloud specific) | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| **Working with Table service sessions** |
| Session pool | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Limit the number of concurrent sessions on the client) | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Minimum number of sessions in the pool | \+ | \+ | \+ | \+ | \- | \- | \- |
| Warm up the pool to the specified number of sessions when the pool is created | \- | \+ | \- | \- | \+ | \- | \- |
| Background KeepAlive for idle sessions in the pool | \+ | \- | \- | \+ | \+ | \+ | \+ |
| Background closing of idle sessions in the pool (redundant sessions) | \+ | \+ | \+ | \+ | \- | \- | \- |
| Automatic dumping of a session from the pool in case of BAD_SESSION/SESSION_BUSY errors | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Storage of sessions for possible future reuse\~ | \+ | \- | \- | \- | \- | \- | \- |
| Retryer on the session pool (a repeat object is a session) | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Retryer on the session pool (a repeat object is a transaction within a session) | \- | \- | \+ | \- | \- | \- | \+ |
| Graceful session shutdown support ("session-close" in "x-ydb-server-hints" metadata means to "forget" a session and not use it again) | \+ | \+ | \+ | \+ | \- | \- |
| Support for server-side load balancing of sessions (a CreateSession request must contain the "session-balancer" value in the "x-ydb-client-capabilities" metadata header) | \+ | \+ | \+ | \- | \- | \- |
| **Support for YDB data types** |
| Int/Uint(8,16,32,64) | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Int128, UInt128 (not available publicly?) | \- | \- | \- | \- | \- | \- | \- | \- |
| Float,Double | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| Bool | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| String, Bytes | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+/\- |
| Utf8, Text | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| NULL,Optional,Void | \+ | \+ | \+ | \+ | \+ | \+/- | \+ | \+/\- |
| Struct | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| List | \+ | \+ | \+ | \+ | \+ | \+/- | \+ | \+ |
| Set | ? | ? | \- | ? | ? | \- | ? | ? |
| Tuple | \+ | \+ | \+ | \+ | \+ | \+ | \+ | ?\+ |
| Variant\<Struct\>,Variant\<Tuple\> | \+ | \+ | \+ | \+ | \+ | \- | \- | \- |
| Date,DateTime,Timestamp,Interval | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \- |
| TzDate,TzDateTime,TzTimestamp | \+ | \+ | \+ | \+ | \+ | \- | \- | \- |
| DyNumber | \+ | \+ | \+ | \+ | \+ | \- | \- | \- |
| Decimal (120 bits) | \+ | \+ | \+ | \+ | \+ | \+ | \- | \+ |
| Json,JsonDocument,Yson | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+/\- |
| **Scheme client** |
| MakeDirectory | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| RemoveDirectory | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| ListDirectory | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| ModifyPermissions | \+ | \+ | \+ | \- | \+ | \- |  | \+ |
| DescribePath | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| **Table service** |
| CreateSession | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| DeleteSession | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| KeepAlive | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \- |
| CreateTable | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| DropTable | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| AlterTable | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| CopyTable | \+ | \+ | \+ | \+ | \- | \- |  | \+ |
| CopyTables | \+ | \+ | \+ | \- | \- | \- |  | \+ |
| DescribeTable | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| ExplainDataQuery | \+ | \+ | \+ | \+ | \- | \- |  | \+ |
| PrepareDataQuery | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| ExecuteDataQuery | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| \* By default, server cache for all parameter requests (KeepInCache) | \- | \+ | \+ | \+ | \+ | \+ |  | \+ |
| \* A separate option to enable/disable server cache for a specific request | \+ | \+ | \+ | \+ | \+ | \+ |  | \+ |
| \* Truncated result as an error (by default) | \- | \- | \+ | ? | \+ | \- |  | \+ |
| \* Truncated result as an error (as an opt-in, opt-out option) | \- | \- | \+ | ? | \+ | \- |  | \- |
| ExecuteSchemeQuery | \+ | \+ | \+ | \+ | \- | \+ | \+ | \+ |
| BeginTransaction | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| CommitTransaction | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| RollbackTransaction | \+ | \+ | \+ | \+ | \+ | \- |  | \+ |
| DescribeTableOptions | \+ | \+ | \+ | \- | \- | \- |  | \-|
| StreamExecuteScanQuery | \+ | \+ | \+ | \+ | \+ | \+ | \+ | \+ |
| StreamReadTable | \+ | \+ | \+ | \+ | \+ | \+ | \- | \+ |
| BulkUpsert | \+ | \+ | \+ | \+ | \+ | \- | \- | \+ |
| **Operation** |
| Consumed Units from metadata of a response to a grpc-request request (for the user to obtain this) | \+ | \+ | \- | \+ | \+ | \- | \- | \- |
| Obtaining OperationId of the operation for a long-polling status of operation execution | \+ | \+ | \+ | \- | \- | \+ | \- | \- |
| **ScriptingYQL** |
| ExecuteYql | \+ | ? | \+ | \- | \- | \- |  | \+ |
| ExplainYql | \+ | ? | \+ | \- | \- | \- |  | \+ |
| StreamExecuteYql | \+ | ? | \+ | \- | \- | \- |  | \- |
| **Coordination service** |
| CreateNode | \+ | ? | \+ | \- | \- | \- |  | \- |
| AlterNode | \+ | ? | \+ | \- | \- | \- |  | \- |
| DropNode | \+ | ? | \+ | \- | \- | \- |  | \- |
| DescribeNode | \+ | ? | \+ | \- | \- | \- |  | \- |
| Session (leader election, distributed lock) | \+ | ? | \- | \- | \- | \- |  | \- |
| **Topic service** |
| CreateTopic | \+ | \+ | \+ | \- | \- | \- | \- | \- |
| DescribeTopic | \+ | \+ | \+ | \- | \- | \- | \- | \- |
| AlterTopic | \+ | \- | \+ | \- | \- | \- | \- | \- |
| DropTopic | \+ | \+ | \+ | \- | \- | \- | \- | \- |
| StreamWrite | \+ | \+ | \+ | \- | \- | \- | \- | \- |
| StreamRead | \+ | \+ | \+ | \- | \- | \- | \- | \- |
| **Ratelimiter service** |
| CreateResource | \+ | ? | \+ | \- | \- | \- | \- |
| AlterResource | \+ | ? | \+ | \- | \- | \- | \- |
| DropResource | \+ | ? | \+ | \- | \- | \- | \- |
| ListResources | \+ | ? | \+ | \- | \- | \- | \- |
| DescribeResource | \+ | ? | \+ | \- | \- | \- | \- |
| AcquireResource | \+ | ? | \+ | \- | \- | \- | \- |
| **Monitoring** (sending SDK metrics to the monitoring system) |
| Solomon / Monitoring | \+ | ? | \+ | \- | \- | \- | \- | \- |
| Prometheus | \- | ? | \+ | \- | \- | \- | \- | \- |
| SDK event **logging** | \- | ? | \+ | \+ | \+ | \+ | \+ | \+ |
| SDK event **tracing** |
| in OpenTelemetry | \- | ? | \- | \- | \- | \- | \- | \- |
| in OpenTracing | \- | ? | \+ | \- | \- | \- | \- | \- |
| **Examples** |
| Auth |
| \* token | ? | ? | \+ | \+ | \+ | \+ | \+ | \+ |
| \* anonymous | ? | ? | \+ | \+ | \+ | \+ |  | \+ |
| \* environ | ? | ? | \+ | \+ | \+ | \- |  | \+ |
| \* metadata | ? | ? | \+ | \+ | \+ | \+ |  | \+ |
| \* service_account | ? | ? | \+ | \+ | \+ | \- |  | \+ |
| \* static (username \+ password) | ? | ? | \+ | \+ | \+ | \+ | \+ | \- |
| Basic (series) | \+ | ? | \+ | \+ | \+ | \+ | \+ | \+ |
| Bulk Upsert | \+/- | ? | \+ | \+ | \+ | \- |  | \- |
| Containers (Struct,Variant,List,Tuple) | \- | ? | \+ | \- | \- | \- |  | \- |
| Pagination | \+ | ? | \+ | \+ | \- | \- |  | \- |
| Partition policies | \- | ? | \+ | \- | \- | \- |  | \-|
| Read table | ? | ? | \+ | \- | \+ | \- |  | \+ |
| Secondary index Workaround | \+ | ? | \- | \+ | \- | \- |  | \- |
| Secondary index builtin | \+ | ? | \- | \- | \- | \- |  | \- |
| TTL | \+ | ? | \+ | \- | \- | \- |  | \- |
| TTL Readtable | \+ | ? | \+ | \- | \- | \- |  | \- |
| URL Shortener (serverless yandex function) | ? | ? | \+ | ? | \+ | \- |  | \- |
| Topic reader | \+ | \+ | \- | \- |  | \- |  | \- |
| Topic writer | \- | \+ | \- | \- |  | \- |  | \- |

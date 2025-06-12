## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17691:New UI handler for fetching data from topics. [#17691](https://github.com/ydb-platform/ydb/pull/17691) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 16688:Added to set [grpc compression](https://github.com/grpc/grpc/blob/master/doc/compression_cookbook.md) at channel level. [#16688](https://github.com/ydb-platform/ydb/pull/16688) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18017:Implemented client balancing of partitions when reading using the Kafka protocol (like Kafka itself). Previously, balancing took place on the server. [#18017](https://github.com/ydb-platform/ydb/pull/18017) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17734:Added automatic cleanup of temporary tables and directories during export to S3. [#17734](https://github.com/ydb-platform/ydb/pull/17734) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))

### Bug fixes

* 17627:Fix early object deletion in wide combiner. [#17627](https://github.com/ydb-platform/ydb/pull/17627) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17853:The transaction has entered the EXECUTED state, but has not yet saved it to disk. If the tablet receives a TEvReadSet, it will send a TEvReadSetAck in response. TEvReadSetAck and it will delete the transaction. If the tablet restarts at this point, the transaction will remain in the WAIT_RS state. The tablet should send TEvReadSetAck only after it saves the transaction status to disk. [#17853](https://github.com/ydb-platform/ydb/pull/17853) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* [Fixed](https://github.com/ydb-platform/ydb/pull/18115) [issues](https://github.com/ydb-platform/ydb/issues/18116) with reading messages larger than 6Mb via [Kafka API](./reference/kafka-api). ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 18086:Bug fixes for direct read in topics [#18086](https://github.com/ydb-platform/ydb/pull/18086) ([qyryq](https://github.com/qyryq))
* 18078:The metric value is reset to zero when the `TEvPQ::TEvPartitionCounters` event arrives. Added a re-calculation of the values. [#18078](https://github.com/ydb-platform/ydb/pull/18078) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18006:The PQ tablet forgets about the transaction only after it receives a TEvReadSetAck from all participants. Another shard may be deleted before the PQ completes the transaction (for example, due to a split table). As a result, transactions are executed, but remain in the WAIT_RS_ACKS state. If the PQ tablet sends a TEvReadSet to a tablet that has already been deleted, it receives a TEvClientConnected with the `Dead` flag in response. In this case, we consider that we have received a TEvReadSetAck. [#18006](https://github.com/ydb-platform/ydb/pull/18006) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16797:Fixed altering of max_active_partition property of the topic with YQL query [#16797](https://github.com/ydb-platform/ydb/pull/16797) ([Nikolay Shestakov](https://github.com/nshestakov))

### Performance

* 17712:Cherry pick latest Roaring UDF changes [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Cherry-pick NaiveBulkAnd into Roaring UDF [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant))
* 17756:Limit internal inflight config updates. [#17756](https://github.com/ydb-platform/ydb/pull/17756) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))

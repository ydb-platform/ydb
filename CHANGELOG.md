## Unreleased

### Functionality

* 17114:Improved audit logging for user management operations. The audit logs now include details about user modification actions such as password changes, user blocking, and unblocking, making it easier to troubleshoot login issues. [#17114](https://github.com/ydb-platform/ydb/pull/17114) ([flown4qqqq](https://github.com/flown4qqqq))
* 17691:New UI handler for fetching data from topics [#17691](https://github.com/ydb-platform/ydb/pull/17691) ([FloatingCrowbar](https://github.com/FloatingCrowbar))
* 16688:related to https://github.com/ydb-platform/ydb/issues/16328 [#16688](https://github.com/ydb-platform/ydb/pull/16688) ([Vitalii Gridnev](https://github.com/gridnevvvit))
* 18017:New kafka messages for DescribeConfigs
Added supporting of Kafka Connect [#18017](https://github.com/ydb-platform/ydb/pull/18017) ([Nikolay Shestakov](https://github.com/nshestakov))
* 18001:Added new functionality for transferring data from topics to tables. The functionality is only available in the Enterprise version. [#18001](https://github.com/ydb-platform/ydb/pull/18001) ([Nikolay Shestakov](https://github.com/nshestakov))
* 17734:Added automatic cleanup of temporary tables and directories during export to S3 [#17734](https://github.com/ydb-platform/ydb/pull/17734) ([stanislav_shchetinin](https://github.com/stanislav-shchetinin))

### Bug fixes

* 17313:Fixed CopyTable operation to allow copying tables with all column types present in the source table, regardless of feature flag settings. This resolves an issue where copying tables with certain decimal types would fail after version downgrades. [#17313](https://github.com/ydb-platform/ydb/pull/17313) ([azevaykin](https://github.com/azevaykin))
* 17122:Fixed an rare issue that caused client applications to hang during commit operations. The problem occurred because the `TEvDeletePartition` message could arrive before the `TEvApproveWriteQuota` message. The batch did not send TEvConsumed and this blocked the queue of write quota requests. [#17122](https://github.com/ydb-platform/ydb/pull/17122) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17627:Fix early object deletion in wide combiner. [#17627](https://github.com/ydb-platform/ydb/pull/17627) ([Filitov Mikhail](https://github.com/lll-phill-lll))
* 17520:The SDK user writes messages to the topic in a transaction. It does not wait for confirmation that the messages have been recorded and calls Commit. The processing of this and the following transactions in the PQ tablet stops. When processing the `UserActionAndTransactionEvents` queue, it was not taken into account that the `TEvGetWriteInfoError` message was received. As a result, the transaction remained at the head of the queue and blocked the processing of other operations. [#17520](https://github.com/ydb-platform/ydb/pull/17520) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 17853:The transaction has entered the EXECUTED state, but has not yet saved it to disk. If the tablet receives a TEvReadSet, it will send a TEvReadSetAck in response. TEvReadSetAck and it will delete the transaction. If the tablet restarts at this point, the transaction will remain in the WAIT_RS state. The tablet should send TEvReadSetAck only after it saves the transaction status to disk. [#17853](https://github.com/ydb-platform/ydb/pull/17853) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18115:This PR fixes optimisation in `NKikimr::NRawSocket::TBufferedWriter`, that wrote the entire message directly to socket if message was larger than available space in buffer. When we wrote more than 6mb to socket with SSL enabled, it every time returned -11 (WAGAIN). As a quick fix, we replace sending of an entire message to socket with cutting this message into 1mb chunks and sending them to socket one by one. [#18115](https://github.com/ydb-platform/ydb/pull/18115) ([Andrey Serebryanskiy](https://github.com/a-serebryanskiy))
* 18086:Bug fixes for direct read in topics [#18086](https://github.com/ydb-platform/ydb/pull/18086) ([qyryq](https://github.com/qyryq))
* 18078:The metric value is reset to zero when the `TEvPQ::TEvPartitionCounters` event arrives. Added a re-calculation of the values. [#18078](https://github.com/ydb-platform/ydb/pull/18078) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 18006:The PQ tablet forgets about the transaction only after it receives a TEvReadSetAck from all participants. Another shard may be deleted before the PQ completes the transaction (for example, due to a split table). As a result, transactions are executed, but remain in the WAIT_RS_ACKS state. If the PQ tablet sends a TEvReadSet to a tablet that has already been deleted, it receives a TEvClientConnected with the `Dead` flag in response. In this case, we consider that we have received a TEvReadSetAck. [#18006](https://github.com/ydb-platform/ydb/pull/18006) ([Alek5andr-Kotov](https://github.com/Alek5andr-Kotov))
* 16797:Fixed altering of max_active_partition property of the topic with YQL query [#16797](https://github.com/ydb-platform/ydb/pull/16797) ([Nikolay Shestakov](https://github.com/nshestakov))

### Performance

* 17712:Cherry pick latest Roaring UDF changes [#17712](https://github.com/ydb-platform/ydb/pull/17712) ([jsjant](https://github.com/jsjant))
* 17578:Cherry-pick NaiveBulkAnd into Roaring UDF [#17578](https://github.com/ydb-platform/ydb/pull/17578) ([jsjant](https://github.com/jsjant))
* 17756:Limit internal inflight config updates. [#17756](https://github.com/ydb-platform/ydb/pull/17756) ([Ilnaz Nizametdinov](https://github.com/CyberROFL))

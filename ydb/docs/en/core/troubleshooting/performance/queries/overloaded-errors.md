# Overloaded errors

{{ ydb-short-name }} returns `OVERLOADED` errors in the following cases:

* Due to an overloaded table partition:
 1. The partition has exceeded the limit on the number of simultaneously processed transactions. For data transactions, this usually manifests as exceeding the threshold of 15000 in-flight requests per shard.

 2. The shard is lagging behind in completing already accepted transactions. This happens when it accumulates a tail of unfinished operations and new requests are temporarily rejected to avoid increasing the backlog.

 3. The shard's local database is overloaded: for example, there are too many transactions already in progress, in-memory data has grown too much, or compaction cannot keep up with the load. In this case, the shard may start rejecting some new requests even if the limit on the number of in-flight transactions at the data shard level has not yet been reached.

 As a rule, these reasons arise due to the same root causes: uneven load distribution, too high write frequency, or lack of resources.

* Due to the table partition being temporarily not in an operational state:
 1. The partition may be splitting or merging, restarting, has not yet completed the service transition to the operational state, or is in another intermediate state in which new requests are temporarily not accepted.

* Due to an overloaded [CDC](../../../concepts/glossary.md#cdc):
 1. The CDC outbound queue size limit has been exceeded: 10000 elements or 125 MB.

* Due to an overloaded node:
 1. The number of open sessions with the {{ ydb-short-name }} node has reached the limit of 1000. This may indicate a problem in the application logic.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/overloaded-errors.md) %}

## Recommendations

If a YQL query returns an `OVERLOADED` error, retry the query using a randomized exponential backoff strategy. The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../reference/ydb-sdk/error_handling.md).

Exceeding the limit of open sessions per node may indicate a problem in the application logic.

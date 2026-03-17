# Overloaded errors

{{ ydb-short-name }} returns `OVERLOADED` errors in the following cases:

* Overloaded table partitions with over 15000 queries in their queue.

* The outbound [CDC](../../../concepts/glossary.md#cdc) queue exceeds the limit of 10000 elements or 125 MB.

* Table partitions in states other than normal, for example partitions in the process of splitting or merging.

* The number of sessions with a {{ ydb-short-name }} node has reached the limit of 1000.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/overloaded-errors.md) %}

## Recommendations

If a YQL query returns an `OVERLOADED` error, retry the query using a randomized exponential back-off strategy. The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../reference/ydb-sdk/error_handling.md).

Exceeding the limit of open sessions per node may indicate a problem in the application logic.

## Cascade effect with incorrect timeouts

Aggressive query retries can cause a cascade effect: when the timeout is shorter than the query execution time, the client starts retrying, but the original queries are not canceled on the server. Each subsequent retry takes longer, exceeding the timeout, and at some point the number of in-flight requests on the shard reaches a critical value. The shard starts responding with `OVERLOADED` to reduce the load, but if clients continue to actively retry, a state of constant overload occurs.

In YDB's actor model, canceling in-flight requests has limitations, especially with client timeouts (when the client closes the gRPC stream before receiving a response). In this state, the shard can only recover through temporary complete load removal, shard restart, or load-based splitting.

Aggressive retries in an overloaded state have the opposite effect - the load on the shard increases by orders of magnitude.
It is recommended to either avoid retrying timeouts altogether or use a sufficiently large exponential backoff to ensure the load actually decreases.
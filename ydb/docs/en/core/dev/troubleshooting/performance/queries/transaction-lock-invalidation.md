# Transaction lock invalidation

Each transaction in {{ ydb-short-name }} uses [optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to ensure that no other transaction has modified the data it has read or changed. If the locks check reveals conflicting modifications, the committing transaction rolls back and must be restarted. In this case, {{ ydb-short-name }} returns a **transaction locks invalidated** error. Restarting a significant share of transactions can degrade your application's performance.

{% note info %}

The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../../reference/ydb-sdk/error_handling.md).

{% endnote %}


## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/transaction-lock-invalidation.md) %}

## Recommendations

The longer a transaction lasts, the higher the likelihood of encountering a **transaction locks invalidated** error.

If possible, avoid interactive transactions. For example, try to avoid the following pattern:

1. Select some data.
1. Process the selected data in the application.
1. Update some data in the database.
1. Commit the transaction in a separate query.

A better approach is to use a single YQL query to select data, update data, and commit the transaction.

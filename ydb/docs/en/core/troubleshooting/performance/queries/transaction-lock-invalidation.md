# Transaction lock invalidation

{{ ydb-short-name }} uses [optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to find conflicts with other transactions being executed. If the locks check during the commit phase reveals conflicting modifications, the committing transaction rolls back and must be restarted. In this case, {{ ydb-short-name }} returns a **transaction locks invalidated** error. Restarting a significant share of transactions can degrade your application's performance.

{% note info %}

The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../reference/ydb-sdk/error_handling.md).

{% endnote %}


## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/transaction-lock-invalidation.md) %}

## Recommendations

Consider the following recommendations:

- The longer a transaction lasts, the higher the likelihood of encountering a **transaction locks invalidated** error.

    If possible, avoid [interactive transactions](../../../concepts/glossary.md#interactive-transaction). A better approach is to use a single YQL query with `begin;` and `commit;` to select data, update data, and commit the transaction.

    If you do need interactive transactions, perform `commit` in the last query in the transaction.

- Analyze the range of primary keys where conflicting modifications occur, and try to change the application logic to reduce the number of conflicts.

    For example, if a single row with a total balance value is frequently updated, split this row into a hundred rows and calculate the total balance as a sum of these rows. This will drastically reduce the number of **transaction locks invalidated** errors.

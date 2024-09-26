# Transaction lock invalidation

## Description

Each transaction in {{ ydb-short-name }} uses [optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to ensure that no other transaction has modified the data it has read or modified. If the check reveals conflicting modifications, the committing transaction rolls back and has to be restarted. In this case {{ ydb-short-name }} returns a *transaction locks invalidated* error. Having to restart a significant share of transactions might decrease your application performance.

## Diagnostics

{% include notitle [#](_includes/transaction-lock-invalidation.md) %}

## Recommendations

The longer a transaction lasts, the higher the chance of getting a *transaction locks invalidated* error.

Avoid interactive transactions. For example, try to avoid this pattern:

* Select some data
* Process the selected data in an application
* Update some data on the database
* Commit the transaction in a separate message

A better approach is to use a single YQL-query for selecting data, updating data, and committing a transaction.

# Handling errors

When using the {{ ydb-short-name }} SDK, you need to handle errors properly.

Errors can be divided into three categories:

* **Temporary failures** (retryable) include a short-term loss of network connectivity, temporary unavailability, overload of a {{ ydb-short-name }} subsystem, or a failure of {{ ydb-short-name }} to respond to a query within the set timeout. If one of these errors occurs, a retry of the failed query is likely to be successful after a certain period of time.

* **Errors that cannot be fixed with a retry** (non-retryable) include incorrectly written queries, {{ ydb-short-name }} internal errors, or queries that mismatch the data schema. Retrying such queries will not resolve the issue. This situation requires developer attention.

* **Errors that can presumably be fixed with a retry after the client application response** (conditionally retryable) include no response within the set timeout or an authentication request.

## Handling retryable errors

The {{ ydb-short-name }} SDK provides [a built-in mechanism for handling temporary failures](../../recipes/ydb-sdk/retry.md). By default, the SDK uses the recommended retry policy that can be changed to meet the requirements of the client app. {{ ydb-short-name }} returns termination codes that let you determine whether a retry is appropriate and which interval to select.

You should retry an operation only if an error refers to a temporary failure. Do not retry invalid operations, such as inserting a row with an existing primary key value into a table or inserting data that mismatches the table schema.

It is extremely important to optimize the number of retries and the interval between them. An excessive number of retries and too short an interval between them result in excessive load. An insufficient number of retries prevents the operation from completion.

When selecting an interval, the following strategies are usually used:

* Exponential backoff. For each subsequent attempt, the interval increases exponentially.
* Intervals in increments. For each subsequent attempt, the interval increases in certain increments.
* Constant intervals. Retries are made at the same intervals.
* Instant retry. Retries are made immediately.
* Random selection. Retries are made after a randomly selected time interval.

When you select an interval and the number of retries, consider the {{ ydb-short-name }} [status codes](./status-codes.md).

Do not use endless retries. This may result in excessive load.

Do not repeat instant retries more than once.

## Logging errors {#log-errors}

When using the SDK, we recommend logging all errors and exceptions:

* Log the number of retries made. An increase in the number of regular retries often indicates some issues.
* Log all errors, including their types, termination codes, and their causes.
* Log the total operation execution time, including operations that terminate after retries.

## See also

- [Status codes](./status-codes.md)
- [Questions and answers: Errors](../../faq/errors.md)


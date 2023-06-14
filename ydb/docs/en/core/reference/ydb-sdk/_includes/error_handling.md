# Handling errors

When using the {{ ydb-short-name }} SDK, there are situations where errors need to be handled.

Errors can be divided into three categories:

* **Temporary failures** (retryable, hereinafter — R): Include a short-term loss of network connectivity, temporary unavailability, or overload of a {{ ydb-short-name }} subsystem, or a failure of {{ ydb-short-name }} to respond to a query within the set timeout. If one of these errors occurs, a retry of the failed query is likely to be successful after a certain period of time.

* **Errors that can't be fixed with a retry** (non retryable, hereinafter — N): Include incorrectly written queries, {{ ydb-short-name }} internal errors, or queries that mismatch the data schema. There is no need to retry a query. This situation requires additional intervention by the developer.

* **Errors that can presumably be fixed with a retry after the client app response** (conditionally retryable, hereinafter — C): Include no response within the set timeout or an authentication request.

The {{ ydb-short-name }} SDK provides a built-in mechanism for handling temporary failures. By default, the SDK uses the recommended retry policy that can be changed to meet the requirements of the client app. {{ ydb-short-name }} returns termination codes that let you determine whether a retry is appropriate and which interval to select.

You should retry an operation only if an error refers to a temporary failure. Don't attempt to retry invalid operations, such as inserting a row with an existing primary key value into a table or inserting data that mismatches the table schema.

It's extremely important to optimize the number of retries and the interval between them. An excessive number of retries and too short an interval between them cause excessive load. An insufficient number of retries prevents the operation from completion.

When selecting an interval, the following strategies are usually used:

* Exponential backoff. For each subsequent attempt, the interval increases exponentially.
* Intervals in increments. For each subsequent attempt, the interval increases in certain increments.
* Constant intervals. Retries are made at the same intervals.
* Instant retry. Retries are made immediately.
* Random selection. Retries are made after a randomly selected time interval.

When you select an interval and the number of retries, consider the {{ ydb-short-name }} termination statuses.

Don't use endless retries. This may cause an excessive load.

Don't repeat instant retries more than once.

## Logging errors {#log-errors}

When using the SDK, we recommend logging all errors and exceptions:

* Log the number of retries made. An increase in the number of regular retries often indicates that there are issues.
* Log all errors, including their types, termination codes, and their causes.
* Log the total operation execution time, including operations that terminate after retries.

## Termination statuses {#termination-statuses}

Below are termination statuses that can be returned when working with the SDK.

Error types:

* R (retryable): Temporary failures
* N (non retryable): Errors that can't be fixed by a retry
* C (conditionally retryable): Errors that can presumably be fixed by a retry after the client app's response is received

| Status | Description | Response | Type |
| :--- | :--- | :--- | :---: |
| SUCCESS | The query was processed successfully | Continue execution |  |
| BAD_REQUEST | Error in query syntax, required fields missing | Check the query | N |
| INTERNAL_ERROR | Unknown internal error | Contact the developers | N |
| ABORTED | The operation was aborted (for example, due to lock invalidation, TRANSACTION_LOCKS_INVALIDATE in detailed error messages) | Retry the entire transaction | R |
| UNAUTHENTICATED | Authentication is required. | Check the token in use. If the token is valid, retry the query. | N |
| UNAUTHORIZED | Access to the requested object (table, directory) is denied | Request access from the DB administrator. | N |
| UNAVAILABLE | Part of the system is not available | Retry the last action (query) | R |
| UNDETERMINED | Unknown transaction status. The query ended with a failure, which made it impossible to determine the status of the transaction. Queries that terminate with this status are subject to transaction integrity and atomicity guarantees. That is, either all changes are registered or the entire transaction is canceled. | For idempotent transactions, you can retry the entire transaction after a small delay. Otherwise, the response depends on the application logic. | C |
| OVERLOADED | Part of the system is overloaded | Retry the last action (query), reduce the rate of queries | R |
| SCHEME_ERROR | The query doesn't match the schema | Fix the query or schema | N |
| GENERIC_ERROR | An unclassified error, possibly related to the query | See the detailed error message and contact the developers | N |
| TIMEOUT | The query timeout expired | Can be repeated in case of idempotent queries | C |
| BAD_SESSION | This session is no longer available | Re-create a session | R |
| PRECONDITION_FAILED | The query cannot be executed for the current state (for example, inserting data into a table with an existing key) | Fix the state or query and retry | C |
| TRANSPORT_UNAVAILABLE | A transport error, the endpoint is unavailable, or the connection was interrupted and can't be reestablished | Check the endpoint or other network settings | C |
| CLIENT_RESOURCE_EXHAUSTED | There are not enough resources available to fulfill the query | Reduce the rate of queries and check client balancing | R |
| CLIENT_DEADLINE_EXCEEDED | The query wasn't processed during the specified client timeout, a different network issue | Check the correctness of the specified timeout, network access, endpoint, or other network settings, reduce the rate of queries, and optimize them | C |


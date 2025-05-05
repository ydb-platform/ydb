# Handling errors

You need to handle errors properly when using the {{ ydb-short-name }} SDK.

Errors can be divided into three categories:

* {% include [retryable](./_includes/tooltips/retryable.md) %}

* {% include [non-retryable](./_includes/tooltips/nonretryable.md) %}

* {% include [conditionally-retryable](./_includes/tooltips/condretryable.md) %}


## Handling retryable errors {#handling-retryable-errors}

The {{ ydb-short-name }} SDK provides [a built-in mechanism for handling temporary failures](../../recipes/ydb-sdk/retry.md). By default, the SDK uses the recommended retry policy, which can be changed to meet the requirements of the client application. {{ ydb-short-name }} returns status codes that let you determine whether a retry is appropriate and which interval to select.

You should retry an operation only if an error refers to a temporary failure. Do not retry invalid operations, such as inserting a row with an existing primary key value into a table or inserting data that mismatches the table schema.

It is extremely important to optimize the number of retries and the interval between them. An excessive number of retries and too short an interval between them result in excessive load. An insufficient number of retries prevents the operation from completing.

The built-in retry mechanisms in {{ ydb-short-name }} SDKs use the following backoff strategies depending on the returned status code:

* **Instant retry** – Retries are made immediately.
* **Fast exponential backoff** – The initial interval is several milliseconds. For each subsequent attempt, the interval increases exponentially.
* **Slow exponential backoff** – The initial interval is several seconds. For each subsequent attempt, the interval increases exponentially.

When selecting an interval manually, the following strategies are usually used:

* **Exponential backoff** – For each subsequent attempt, the interval increases exponentially.
* **Intervals in increments** – For each subsequent attempt, the interval increases in certain increments.
* **Constant intervals** – Retries are made at the same intervals.
* **Instant retry** – Retries are made immediately.
* **Random selection** – Retries are made after a randomly selected time interval.

When selecting an interval and the number of retries, consider the {{ ydb-short-name }} [status codes](#status-codes).

Do not use endless retries, as this may result in excessive load.

Do not repeat instant retries more than once.

For code samples, see [{#T}](../../recipes/ydb-sdk/retry.md).

## Status codes {#status-codes}

When an error occurs, the {{ ydb-short-name }} SDK returns an error object that includes status codes. The returned status code may come from the YDB server, gRPC transport, or the SDK itself.

Status codes within the range of 400000-400999 are {{ ydb-short-name }}  server codes that are identical for all {{ ydb-short-name }}  SDKs. Refer to [Status codes from the YDB server](./ydb-status-codes.md).

Status codes within the range of 401000-401999 are SDK-specific. For more information about SDK-specific codes, refer to the corresponding SDK documentation.

For more information about gRPC status codes, see the [gRPC documentation](https://grpc.io/docs/guides/status-codes/).

## Logging errors {#log-errors}

When using the SDK, we recommend logging all errors and exceptions:

* Log the number of retries made. An increase in the number of regular retries often indicates issues.
* Log all errors, including their types, termination codes, and causes.
* Log the total operation execution time, including operations that terminate after retries.

## See also

- [gRPC status codes](./grpc-status-codes.md)
- [{{ ydb-short-name }} server status codes](./ydb-status-codes.md)
- [Questions and answers: Errors](../../faq/errors.md)

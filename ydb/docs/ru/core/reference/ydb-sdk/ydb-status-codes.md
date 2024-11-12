# Статусы завершения {{ ydb-short-name }} сервера


## 400000: SUCCESS

The query was processed successfully.

No response required. Continue application execution.

<div class="tags_list">

## 400010: BAD_REQUEST

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Invalid query syntax or required fields were missing.

Correct the query.

<div class="tags_list">

## 400020: UNAUTHORIZED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Access to the requested object (table, directory) is denied.

Request access from the DB administrator.

<div class="tags_list">

## 400030: INTERNAL_ERROR

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

An unknown internal error occurred.

Contact the developers.

<div class="tags_list">

## 400040: ABORTED

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

The operation was aborted. Possible reasons might include lock invalidation, TRANSACTION_LOCKS_INVALIDATE in detailed error messages.

Retry the entire transaction.

<div class="tags_list">

## 400050: UNAVAILABLE

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

A part of the system is not available.

Retry the last action (query).

<div class="tags_list">

## 400060: OVERLOADED

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

A part of the system is overloaded.

Retry the last action (query), reduce the rate of queries.

<div class="tags_list">

## 400070: SCHEME_ERROR

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The query does not match the schema.

Correct the query or schema.

<div class="tags_list">

## 400080: GENERIC_ERROR

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

An unclassified error occurred, possibly related to the query.

See the detailed error message and contact the developers.

<div class="tags_list">

## 400090: TIMEOUT

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

The query timeout expired.

If idempotent, retry the query.

<div class="tags_list">

## 400100: BAD_SESSION

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

This session is no longer available.

Re-create a session.

<div class="tags_list">

## 400120: PRECONDITION_FAILED

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

The query cannot be executed for the current state. For example, inserting data into a table with an existing key.

Correct the state or query and retry.

<div class="tags_list">

## 400130: ALREADY_EXISTS

[//]: # (TODO: Verify the description)
{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The database object being created already exists in the {{ ydb-short-name }} cluster.

The response depends on the application logic.

<div class="tags_list">

## 400140: NOT_FOUND

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The database object is not fond in the {{ ydb-short-name }} database.

The response depends on the application logic.

<div class="tags_list">

## 400150: SESSION_EXPIRED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The session has already expired.

Re-create a session.

<div class="tags_list">

## 400160: CANCELLED

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

<div class="tags_list">

## 400170: UNDETERMINED

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

An unknown transaction status. The query ended with a failure, which made it impossible to determine the status of the transaction. Queries that terminate with this status are subject to transaction integrity and atomicity guarantees. That is, either all changes are registered or the entire transaction is canceled.

For idempotent transactions, you can retry the entire transaction after a small delay. Otherwise, the response depends on the application logic.

<div class="tags_list">

## 400180: UNSUPPORTED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>


<div class="tags_list">

## 400190: SESSION_BUSY

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

The session is busy.

Re-create the session.

<div class="tags_list">

## 400200: EXTERNAL_ERROR

</div>


## Дополнительная информация

[Вопросы и ответы: Ошибки](../../faq/errors.md)

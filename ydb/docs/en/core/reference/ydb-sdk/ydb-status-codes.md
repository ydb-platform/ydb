# Status codes from the {{ ydb-short-name }} server

[//]: # (Information from https://GitHub.com/ydb-platform/ydb-go-sdk/blob/master/retry/errors_data_test.go)

#|
||
Code
|
Status
|
Retryability
|
Backoff strategy
|
Recreate session
||

||
[400000](#success)
|
[SUCCESS](#success)
|
–
|
–
|
–
||

||
[400010](#bad-request)
|
[BAD_REQUEST](#bad-request)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400020](#unauthorized)
|
[UNAUTHORIZED](#unauthorized)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400030](#internal-error)
|
[INTERNAL_ERROR](#internal-error)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400040](#aborted)
|
[ABORTED](#aborted)
|
[retryable](*retryable)
|
[fast](*fastbackoff)
|
no
||

||
[400050](#unavailable)
|
[UNAVAILABLE](#unavailable)
|
[retryable](*retryable)
|
[fast](*fastbackoff)
|
no
||

||
[400060](#overloaded)
|
[OVERLOADED](#overloaded)
|
[retryable](*retryable)
|
[slow](*slowbackoff)
|
no
||

||
[400070](#scheme-error)
|
[SCHEME_ERROR](#scheme-error)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400080](#generic-error)
|
[GENERIC_ERROR](#generic-error)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400090](#timeout)
|
[TIMEOUT](#timeout)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400100](#bad-session)
|
[BAD_SESSION](#bad-session)
|
[retryable](*retryable)
|
[instant](*instant)
|
yes
||

||
[400120](#precondition-failed)
|
[PRECONDITION_FAILED](#precondition-failed)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400130](#already-exists)
|
[ALREADY_EXISTS](#already-exists)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400140](#not-found)
|
[NOT_FOUND](#not-found)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400150](#session-expired)
|
[SESSION_EXPIRED](#session-expired)
|
[retryable](*retryable)
|
[instant](*instant)
|
yes
||

||
[400160](#cancelled)
|
[CANCELLED](#cancelled)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400170](#undetermined)
|
[UNDETERMINED](#undetermined)
|
[conditionally-retryable](*condretryable)
|
[fast](*fastbackoff)
|
no
||

||
[400180](#unsupported)
|
[UNSUPPORTED](#unsupported)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[400190](#session-busy)
|
[SESSION_BUSY](#session-busy)
|
[retryable](*retryable)
|
[fast](*fastbackoff)
|
yes
||

||
[400200](#external-error)
|
[EXTERNAL_ERROR](#external-error)
|
[non-retryable](*nonretryable)
|
–
|
no
||
|#

## 400000: SUCCESS {#success}

The query was processed successfully.

No response is required. Continue application execution.

<div class="tags_list">

## 400010: BAD_REQUEST {#bad-request}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Invalid query syntax or missing required fields.

Correct the query.

<div class="tags_list">

## 400020: UNAUTHORIZED {#unauthorized}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Access to the requested schema object (for example, a table or directory) is denied.

Request access from its owner.

<div class="tags_list">

## 400030: INTERNAL_ERROR {#internal-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

An unknown internal error occurred.

File a [GitHub issue](https://github.com/ydb-platform/ydb/issues/new) or contact {{ ydb-short-name }} technical support.

<div class="tags_list">

## 400040: ABORTED {#aborted}

{% include notitle [retryable-fast](./_includes/tags.md#retryable-fastbackoff) %}

</div>

The operation was aborted. Possible reasons might include lock invalidation with `TRANSACTION_LOCKS_INVALIDATED` in detailed error messages.

Retry the entire transaction.

<div class="tags_list">

## 400050: UNAVAILABLE {#unavailable}

{% include notitle [retryable-fastbackoff](./_includes/tags.md#retryable-fastbackoff) %}

</div>

A part of the system is not available.

Retry the last action (query).

<div class="tags_list">

## 400060: OVERLOADED {#overloaded}

{% include notitle [retryable-slowbackoff](./_includes/tags.md#retryable-slowbackoff) %}

</div>

A part of the system is overloaded.

Retry the last action (query) and reduce the query rate.

<div class="tags_list">

## 400070: SCHEME_ERROR {#scheme-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The query does not match the schema.

Correct the query or schema.

<div class="tags_list">

## 400080: GENERIC_ERROR {#generic-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

An unclassified error occurred, possibly related to the query.

See the detailed error message. If necessary, file a [GitHub issue](https://github.com/ydb-platform/ydb/issues/new) or contact {{ ydb-short-name }} technical support.

<div class="tags_list">

## 400090: TIMEOUT {#timeout}

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

The query timeout expired.

If the query is idempotent, retry it.

<div class="tags_list">

## 400100: BAD_SESSION {#bad-session}

{% include notitle [retryable-instant](./_includes/tags.md#retryable) %}

</div>

This session is no longer available.

Create a new session.

<div class="tags_list">

## 400120: PRECONDITION_FAILED {#precondition-failed}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The query cannot be executed in the current state. For example, inserting data into a table with an existing key.

Correct the state or query, then retry.

<div class="tags_list">

## 400130: ALREADY_EXISTS {#already-exists}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The database object being created already exists in the {{ ydb-short-name }} cluster.

The response depends on the application logic.

<div class="tags_list">

## 400140: NOT_FOUND {#not-found}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The database object was not found in the {{ ydb-short-name }} database.

The response depends on the application logic.

<div class="tags_list">

## 400150: SESSION_EXPIRED {#session-expired}

{% include notitle [conditionally-retryable-instant](./_includes/tags.md#conditionally-retryable) %}

</div>

The session has already expired.

Create a new session.

<div class="tags_list">

## 400160: CANCELLED {#cancelled}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The request was canceled on the server. For example, a user canceled a long-running query in the [Embedded UI](../embedded-ui/index.md), or the query included the [cancel_after](../../dev/timeouts.md#cancel) timeout option.

If the query took too long to complete, try optimizing it. If you used the `cancel_after` timeout option, increase the timeout value.

<div class="tags_list">

## 400170: UNDETERMINED {#undetermined}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

An unknown transaction status. The query ended with a failure, making it impossible to determine the transaction status. Queries that terminate with this status are subject to transaction integrity and atomicity guarantees. That is, either all changes are registered, or the entire transaction is canceled.

For idempotent transactions, retry the entire transaction after a short delay. Otherwise, the response depends on the application logic.

<div class="tags_list">

## 400180: UNSUPPORTED {#unsupported}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The query is not supported by {{ ydb-short-name }} either because support for such queries is not yet implemented or is not enabled in the {{ ydb-short-name }} configuration.

Correct the query or enable support for such queries in {{ ydb-short-name }}.

<div class="tags_list">

## 400190: SESSION_BUSY {#session-busy}

{% include notitle [retryable-fastbackoff](./_includes/tags.md#retryable-fastbackoff) %}

</div>

The session is busy.

Create a new session.

<div class="tags_list">

## 400200: EXTERNAL_ERROR {#external-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

An error occurred in an external system, for example, when processing a federated query or importing data from an external data source.

See the detailed error message. If necessary, file a [GitHub issue](https://github.com/ydb-platform/ydb/issues/new) or contact {{ ydb-short-name }} technical support.


## See also

[Questions and answers: Errors](../../faq/errors.md)


[*instant]: {% include [instant](./_includes/tooltips/instant.md) %}

[*fastbackoff]: {% include [fast backoff](./_includes/tooltips/fast_backoff.md) %}

[*slowbackoff]: {% include [slow backoff](./_includes/tooltips/slow_backoff.md) %}

[*retryable]: {% include [retryable](./_includes/tooltips/retryable.md) %}

[*nonretryable]: {% include [nonretryable](./_includes/tooltips/nonretryable.md) %}

[*condretryable]: {% include [conditionally retryable](./_includes/tooltips/condretryable.md) %}

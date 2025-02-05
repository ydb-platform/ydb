# gRPC status codes

{{ ydb-short-name }}  provides the gRPC API, which you can use to manage your database resources and data. The following table describes the gRPC status codes:

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
[0](#ok)
|
[OK](#ok)
|
–
|
–
|
–
||

||
[1](#cancelled)
|
[CANCELLED](#cancelled)
|
[conditionally-retryable](*condretryable)
|
[fast](*fastbackoff)
|
yes
||

||
[2](#unknown)
|
[UNKNOWN](#unknown)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[3](#invalid-argument)
|
[INVALID_ARGUMENT](#invalid-argument)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[4](#deadline-exceeded)
|
[DEADLINE_EXCEEDED](#deadline-exceeded)
|
[conditionally-retryable](*condretryable)
|
[fast](*fastbackoff)
|
yes
||

||
[5](#not-found)
|
[NOT_FOUND](#not-found)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[6](#already-exists)
|
[ALREADY_EXISTS](#already-exists)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[7](#permission-denied)
|
[PERMISSION_DENIED](#permission-denied)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[8](#resource-exhausted)
|
[RESOURCE_EXHAUSTED](#resource-exhausted)
|
[retryable](*retryable)
|
[slow](*slowbackoff)
|
no
||

||
[9](#failed-precondition)
|
[FAILED_PRECONDITION](#failed-precondition)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[10](#aborted)
|
[ABORTED](#aborted)
|
[retryable](*retryable)
|
[instant](*instant)
|
yes
||

||
[11](#out-of-range)
|
[OUT_OF_RANGE](#out-of-range)
|
[non-retryable](*nonretryable)
|
–
|
no
||

||
[12](#unimplemented)
|
[UNIMPLEMENTED](#unimplemented)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[13](#internal)
|
[INTERNAL](#internal)
|
[conditionally-retryable](*condretryable)
|
[fast](*fastbackoff)
|
yes
||

||
[14](#unavailable)
|
[UNAVAILABLE](#unavailable)
|
[conditionally-retryable](*condretryable)
|
[fast](*fastbackoff)
|
yes
||

||
[15](#data-loss)
|
[DATA_LOSS](#data-loss)
|
[non-retryable](*nonretryable)
|
–
|
yes
||

||
[16](#unauthenticated)
|
[UNAUTHENTICATED](#unauthenticated)
|
[non-retryable](*nonretryable)
|
–
|
yes
||
|#

## 0: OK {#ok}

Not an error; returned on success.

<div class="tags_list">

## 1: CANCELLED {#cancelled}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

The operation was cancelled, typically by the caller.

<div class="tags_list">

## 2: UNKNOWN {#unknown}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Unknown error. For example, this error may be returned when a `Status` value received from another address space belongs to an error space that is not known in this address space. Errors raised by APIs that do not return enough error information may also be converted to this error.

<div class="tags_list">

## 3: INVALID_ARGUMENT {#invalid-argument}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The client specified an invalid argument. Unlike `FAILED_PRECONDITION`, `INVALID_ARGUMENT` indicates arguments that are problematic regardless of the system state (e.g., a malformed file name).

<div class="tags_list">

## 4: DEADLINE_EXCEEDED {#deadline-exceeded}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

The query was not processed within the specified client timeout, or a network issue occurred.

Check the specified timeout, network access, endpoint, and other network settings. Reduce the query rate and optimize queries.

<div class="tags_list">

## 5: NOT_FOUND {#not-found}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

A requested scheme object (for example, a table or directory) was not found.

<div class="tags_list">

## 6: ALREADY_EXISTS {#already-exists}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The scheme object that a client attempted to create (e.g., file or directory) already exists.

<div class="tags_list">

## 7: PERMISSION_DENIED {#permission-denied}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The caller does not have permission to execute the specified operation.

<div class="tags_list">

## 8: RESOURCE_EXHAUSTED {#resource-exhausted}

{% include notitle [retryable-slowbackoff](./_includes/tags.md#retryable-slowbackoff) %}

</div>

There are not enough resources available to fulfill the query.

Reduce the query rate and check client balancing.

<div class="tags_list">

## 9: FAILED_PRECONDITION {#failed-precondition}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The query cannot be executed in the current state (for example, inserting data into a table with an existing key).

Fix the state or query, then retry.

<div class="tags_list">

## 10: ABORTED {#aborted}

{% include notitle [retryable-instant](./_includes/tags.md#retryable) %}

</div>

The operation was aborted, typically due to a concurrency issue, such as a transaction abort.

<div class="tags_list">

## 11: OUT_OF_RANGE {#out-of-range}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The operation was attempted past the valid range. Unlike `INVALID_ARGUMENT`, this error indicates a problem that may be fixed if the system state changes.

<div class="tags_list">

## 12: UNIMPLEMENTED {#unimplemented}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The operation is not implemented, supported, or enabled in this service.

<div class="tags_list">

## 13: INTERNAL {#internal}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Internal errors. This means that some invariants expected by the underlying system have been broken. This error code is reserved for significant problems.

<div class="tags_list">

## 14: UNAVAILABLE {#unavailable}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

The service is currently unavailable. This is most likely a transient condition that can be corrected by retrying with a backoff. Note that it is not always safe to retry non-idempotent operations.

<div class="tags_list">

## 15: DATA_LOSS {#data-loss}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Unrecoverable data loss or corruption.

<div class="tags_list">

## 16: UNAUTHENTICATED {#unauthenticated}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The request did not have valid authentication credentials.

Retry the request with valid authentication credentials.

[*instant]: {% include [instant](./_includes/tooltips/instant.md) %}

[*fastbackoff]: {% include [fast backoff](./_includes/tooltips/fast_backoff.md) %}

[*slowbackoff]: {% include [slow backoff](./_includes/tooltips/slow_backoff.md) %}

[*retryable]: {% include [retryable](./_includes/tooltips/retryable.md) %}

[*nonretryable]: {% include [nonretryable](./_includes/tooltips/nonretryable.md) %}

[*condretryable]: {% include [conditionally retryable](./_includes/tooltips/condretryable.md) %}

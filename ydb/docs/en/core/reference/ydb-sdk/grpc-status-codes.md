# gRPC status codes

YDB provides the gRPC API, which you can use to manage your DB resources and data. The following table describes the gRPC status codes:

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
conditionally-retryable
|
fast
|
yes
||

||
[2](#unknown)
|
[UNKNOWN](#unknown)
|
non-retryable
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
non-retryable
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
conditionally-retryable
|
fast
|
yes
||

||
[5](#not-found)
|
[NOT_FOUND](#not-found)
|
non-retryable
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
non-retryable
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
non-retryable
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
retryable
|
slow
|
no
||

||
[9](#failed-precondition)
|
[FAILED_PRECONDITION](#failed-precondition)
|
non-retryable
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
retryable
|
instant
|
yes
||

||
[11](#out-of-range)
|
[OUT_OF_RANGE](#out-of-range)
|
non-retryable
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
non-retryable
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
conditionally-retryable
|
fast
|
yes
||

||
[14](#unavailable)
|
[UNAVAILABLE](#unavailable)
|
conditionally-retryable
|
fast
|
yes
||

||
[15](#data-loss)
|
[DATA_LOSS](#data-loss)
|
non-retryable
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
non-retryable
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

Unknown error. For example, this error may be returned when a `Status` value received from another address space belongs to an error space that is not known in this address space. Also errors raised by APIs that do not return enough error information may be converted to this error.

<div class="tags_list">

## 3: INVALID_ARGUMENT {#invalid-argument}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The client specified an invalid argument. Note that this differs from `FAILED_PRECONDITION`. `INVALID_ARGUMENT` indicates arguments that are problematic regardless of the state of the system (e.g., a malformed file name).

<div class="tags_list">

## 4: DEADLINE_EXCEEDED {#deadline-exceeded}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

The query was not processed during the specified client timeout or a network issue occurred.

Check the correctness of the specified timeout, network access, endpoint, or other network settings, reduce the rate of queries, and optimize them.

<div class="tags_list">

## 5: NOT_FOUND {#not-found}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Some requested database object (e.g., table or directory) was not found.

<div class="tags_list">

## 6: ALREADY_EXISTS {#already-exists}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The entity that a client attempted to create (e.g., file or directory) already exists.

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

Reduce the rate of queries and check client balancing.

<div class="tags_list">

## 9: FAILED_PRECONDITION {#failed-precondition}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The query cannot be executed for the current state (for example, inserting data into a table with an existing key).

Fix the state or query and retry.

<div class="tags_list">

## 10: ABORTED {#aborted}

{% include notitle [retryable-instant](./_includes/tags.md#retryable) %}

</div>

The operation was aborted, typically due to a concurrency issue such as a transaction abort.

<div class="tags_list">

## 11: OUT_OF_RANGE {#out-of-range}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The operation was attempted past the valid range. Unlike `INVALID_ARGUMENT`, this error indicates a problem that may be fixed if the system state changes.

<div class="tags_list">

## 12: UNIMPLEMENTED {#unimplemented}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The operation is not implemented or is not supported/enabled in this service.

<div class="tags_list">

## 13: INTERNAL {#internal}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Internal errors. This means that some invariants expected by the underlying system have been broken. This error code is reserved for serious errors.

<div class="tags_list">

## 14: UNAVAILABLE {#unavailable}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

The service is currently unavailable. This is most likely a transient condition, which can be corrected by retrying with a backoff. Note that it is not always safe to retry non-idempotent operations.

<div class="tags_list">

## 15: DATA_LOSS {#data-loss}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Unrecoverable data loss or corruption.

<div class="tags_list">

## 16: UNAUTHENTICATED {#unauthenticated}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

The request does not have valid authentication credentials for the operation.

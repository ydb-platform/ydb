# Python SDK status codes

{% include notitle [note](./_includes/ydb-status-codes-note.md) %}

<div class="tags_list">

## 401010: CONNECTION_LOST

</div>

{% include notitle [CONNECTION_LOST](./_includes/statuses/connection-lost.md) %}

<div class="tags_list">

## 401020: CONNECTION_FAILURE

</div>

{% include notitle [CONNECTION_FAILURE](./_includes/statuses/connection-failure.md) %}

<div class="tags_list">

## 401030: DEADLINE_EXCEEDED

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

{% include notitle [DEADLINE_EXCEEDED](./_includes/statuses/deadline-exceeded.md) %}

<div class="tags_list">

## 401040: CLIENT_INTERNAL_ERROR

</div>

{% include notitle [INTERNAL_ERROR](./_includes/statuses/client-internal-error.md) %}

<div class="tags_list">

## 401050: UNIMPLEMENTED

</div>

{% include notitle [unimplemended](./_includes/statuses/client-call-unimplemented.md) %}

<div class="tags_list">

## 402030: UNAUTHENTICATED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [unauthenticated](./_includes/statuses/client-unauthenticated.md) %}


<div class="tags_list">

## 402040: SESSION_POOL_EMPTY

</div>

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [SESSION_POOL_EMPTY](./_includes/statuses/session-pool-empty.md) %}

## See also

[Questions and answers: Errors](../../faq/errors.md)



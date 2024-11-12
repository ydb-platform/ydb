# C++ SDK status codes

{% include notitle [note](./_includes/ydb-status-codes-note.md) %}

<div class="tags_list">

## 401010: TRANSPORT_UNAVAILABLE

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

{% include notitle [TRANSPORT_UNAVAILABLE](./_includes/statuses/transport-unavailable.md) %}


<div class="tags_list">

## 401020: CLIENT_RESOURCE_EXHAUSTED

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

{% include notitle [RESOURCE_EXHAUSTED](./_includes/statuses/resource-exhausted.md) %}


<div class="tags_list">

## 401030: CLIENT_DEADLINE_EXCEEDED

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

{% include notitle [DEADLINE_EXCEEDED](./_includes/statuses/deadline-exceeded.md) %}


<div class="tags_list">

## 401050: CLIENT_INTERNAL_ERROR

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [CLIENT_INTERNAL_ERROR](./_includes/statuses/client-internal-error.md) %}


<div class="tags_list">

## 401060: CLIENT_CANCELLED

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

{% include notitle [CLIENT_CANCELLED](./_includes/statuses/client-cancelled.md) %}


<div class="tags_list">

## 401070: CLIENT_UNAUTHENTICATED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [CLIENT_UNAUTHENTICATED](./_includes/statuses/client-unauthenticated.md) %}


<div class="tags_list">

## 401080: CLIENT_CALL_UNIMPLEMENTED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [CLIENT_CALL_UNIMPLEMENTED](./_includes/statuses/client-call-unimplemented.md) %}


<div class="tags_list">

## 401090: CLIENT_OUT_OF_RANGE

</div>

{% include notitle [CLIENT_OUT_OF_RANGE](./_includes/statuses/client-out-of-range.md) %}

<!--div class="tags_list">

## 402010: CLIENT_DISCOVERY_FAILED

</div-->


<div class="tags_list">

## 402020: CLIENT_LIMITS_REACHED

</div>

{% include notitle [CLIENT_LIMITS_REACHED](./_includes/statuses/client-limits-reached.md) %}


## See also

[Questions and answers: Errors](../../faq/errors.md)



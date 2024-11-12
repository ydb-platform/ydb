# Статусы завершения C# SDK

<div class="tags_list">

## 500010: ClientResourceExhausted

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

{% include notitle [RESOURCE_EXHAUSTED](./_includes/statuses/resource-exhausted.md) %}


<div class="tags_list">

## 500020: ClientInternalError

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

{% include notitle [CLIENT_INTERNAL_ERROR](./_includes/statuses/client-internal-error.md) %}


<div class="tags_list">

## 600010: ClientTransportUnknown

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [TRANSPORT_UNKNOWN](./_includes/statuses/client-transport-unknown.md) %}


<div class="tags_list">

## 600020: ClientTransportUnavailable

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

{% include notitle [TRANSPORT_UNAVAILABLE](./_includes/statuses/transport-unavailable.md) %}


<div class="tags_list">

## 600030: ClientTransportTimeout

<!--{% include notitle [description](path) %}-->

</div>

{% include notitle [TRANSPORT_TIMEOUT](./_includes/statuses/client-transport-timeout.md) %}


<div class="tags_list">

## 600040: ClientTransportResourceExhausted

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

{% include notitle [RESOURCE_EXHAUSTED](./_includes/statuses/client-transport-resource-exhausted.md) %}


<div class="tags_list">

## 600050: ClientTransportUnimplemented

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [description](./_includes/statuses/client-call-unimplemented.md) %}



## Дополнительная информация

[Вопросы и ответы: Ошибки](../../faq/errors.md)

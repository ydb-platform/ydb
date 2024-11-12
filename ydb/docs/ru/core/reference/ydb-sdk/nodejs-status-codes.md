# Статусы завершения NodeJS SDK

{% include notitle [note](./_includes/ydb-status-codes-note.md) %}

<div class="tags_list">

## 401010: TRANSPORT_UNAVAILABLE

</div>

{% include notitle [TRANSPORT_UNAVAILABLE](./_includes/statuses/transport-unavailable.md) %}

<div class="tags_list">

## 401020: CLIENT_RESOURCE_EXHAUSTED

</div>

{% include notitle [RESOURCE_EXHAUSTED](./_includes/statuses/resource-exhausted.md) %}

<div class="tags_list">

## 401030: CLIENT_DEADLINE_EXCEEDED

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

{% include notitle [DEADLINE_EXCEEDED](./_includes/statuses/deadline-exceeded.md) %}

<div class="tags_list">

## 401034: CLIENT_CANCELED

{% include notitle [retryable](./_includes/tags.md#retryable) %}

</div>

{% include notitle [CLIENT_CANCELED](./_includes/statuses/client-cancelled.md) %}

<div class="tags_list">

## 402030: UNAUTHENTICATED

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

{% include notitle [unauthenticated](./_includes/statuses/client-unauthenticated.md) %}

<div class="tags_list">

## 402040: SESSION_POOL_EMPTY

</div>

{% include notitle [SESSION_POOL_EMPTY](./_includes/statuses/session-pool-empty.md) %}

<div class="tags_list">

## 402050: RETRIES_EXCEEDED

</div>

{% include notitle [RETRIES_EXCEEDED](./_includes/statuses/retries-exceeded.md) %}


## Дополнительная информация

[Вопросы и ответы: Ошибки](../../faq/errors.md)

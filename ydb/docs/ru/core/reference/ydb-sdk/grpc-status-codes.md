# Статусы завершения gRPC

{{ ydb-short-name }} предоставляет gRPC API, с помощью которого вы можете управлять ресурсами и данными БД. В следующей таблице описаны статусы завершения запросов gRPC:

#|
||
Код
|
Статус
|
Возможность повтора
|
Стратегия задержек
|
Пересоздать сессию
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
[условно повторяемый](*condretryable)
|
[короткая](*fastbackoff)
|
да
||

||
[2](#unknown)
|
[UNKNOWN](#unknown)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[3](#invalid-argument)
|
[INVALID_ARGUMENT](#invalid-argument)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[4](#deadline-exceeded)
|
[DEADLINE_EXCEEDED](#deadline-exceeded)
|
[условно повторяемый](*condretryable)
|
[короткая](*fastbackoff)
|
да
||

||
[5](#not-found)
|
[NOT_FOUND](#not-found)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[6](#already-exists)
|
[ALREADY_EXISTS](#already-exists)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[7](#permission-denied)
|
[PERMISSION_DENIED](#permission-denied)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[8](#resource-exhausted)
|
[RESOURCE_EXHAUSTED](#resource-exhausted)
|
[повторяемый](*retryable)
|
[большая](*slowbackoff)
|
нет
||

||
[9](#failed-precondition)
|
[FAILED_PRECONDITION](#failed-precondition)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[10](#aborted)
|
[ABORTED](#aborted)
|
[повторяемый](*retryable)
|
[моментально](*instant)
|
да
||

||
[11](#out-of-range)
|
[OUT_OF_RANGE](#out-of-range)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[12](#unimplemented)
|
[UNIMPLEMENTED](#unimplemented)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[13](#internal)
|
[INTERNAL](#internal)
|
[условно повторяемый](*condretryable)
|
[короткая](*fastbackoff)
|
да
||

||
[14](#unavailable)
|
[UNAVAILABLE](#unavailable)
|
[условно повторяемый](*condretryable)
|
[короткая](*fastbackoff)
|
да
||

||
[15](#data-loss)
|
[DATA_LOSS](#data-loss)
|
[неповторяемый](*nonretryable)
|
–
|
да
||

||
[16](#unauthenticated)
|
[UNAUTHENTICATED](#unauthenticated)
|
[неповторяемый](*nonretryable)
|
–
|
да
||
|#

## 0: OK {#ok}

Не является ошибкой. Возвращается при успешном выполнении.

<div class="tags_list">

## 1: CANCELLED {#cancelled}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Операция была отменена, как правило, вызывающей стороной.

<div class="tags_list">

## 2: UNKNOWN {#unknown}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Неизвестная ошибка. Например, эта ошибка может возникнуть, когда значение `Status`, полученное из другого адресного пространства, относится к неизвестному пространству ошибок. Также эта ошибка может быть преобразована из ошибок, вызванных API, которые не предоставляют достаточно информации об ошибке.

<div class="tags_list">

## 3: INVALID_ARGUMENT {#invalid-argument}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Клиент указал недопустимый аргумент. В отличие от `FAILED_PRECONDITION`, `INVALID_ARGUMENT` указывает на аргументы, которые являются проблемными независимо от состояния системы (например, неправильное имя файла).

<div class="tags_list">

## 4: DEADLINE_EXCEEDED {#deadline-exceeded}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Запрос не был обработан за заданный клиентский таймаут или произошла иная сетевая проблема.

Проверьте корректность заданного таймаута, наличие сетевого доступа, правильность endpoint'а и других сетевых настроек. Также рекомендуется снизить интенсивность потока запросов и оптимизировать их.

<div class="tags_list">

## 5: NOT_FOUND {#not-found}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрашиваемый схемный объект (например, таблица или папка) не найден.

<div class="tags_list">

## 6: ALREADY_EXISTS {#already-exists}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Схемный объект, который пытается создать клиент (например, таблица или папка), уже существует.

<div class="tags_list">

## 7: PERMISSION_DENIED {#permission-denied}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

У вызывающего нет разрешения на выполнение указанной операции.

<div class="tags_list">

## 8: RESOURCE_EXHAUSTED {#resource-exhausted}

{% include notitle [retryable-slowbackoff](./_includes/tags.md#retryable-slowbackoff) %}

</div>

Недостаточно свободных ресурсов для обслуживания запроса.

Снизьте интенсивность потока запросов, проверьте клиентскую балансировку.

<div class="tags_list">

## 9: FAILED_PRECONDITION {#failed-precondition}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрос не может быть выполнен в текущем состоянии (например, вставка в таблицу с существующим ключом).

Исправьте состояние или запрос и повторите попытку.

<div class="tags_list">

## 10: ABORTED {#aborted}

{% include notitle [retryable-instant](./_includes/tags.md#retryable) %}

</div>

Операция не выполнена (например, из-за инвалидации локов, `TRANSACTION_LOCKS_INVALIDATE` в подробных сообщениях об ошибке).

Повторите всю транзакцию.

<div class="tags_list">

## 11: OUT_OF_RANGE {#out-of-range}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Была предпринята попытка выполнить операцию за пределами допустимого диапазона. В отличие от `INVALID_ARGUMENT`, эта ошибка указывает на проблему, которая может быть исправлена при изменении состояния системы.

<div class="tags_list">

## 12: UNIMPLEMENTED {#unimplemented}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Операция не реализована или не поддерживается (не активирована) в {{ ydb-short-name }}.

<div class="tags_list">

## 13: INTERNAL {#internal}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Внутренние ошибки. Это означает, что некоторые инварианты, ожидаемые базовой системой, были нарушены. Данный код ошибки предназначен для серьёзных ошибок.

<div class="tags_list">

## 14: UNAVAILABLE {#unavailable}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Сервис в данный момент недоступен. Скорее всего, это временное состояние, которое можно обойти, повторив запрос после некоторой задержки. Обратите внимание, что повторение неидемпотентных операций не всегда безопасно.

<div class="tags_list">

## 15: DATA_LOSS {#data-loss}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Неустранимая потеря или повреждение данных.

<div class="tags_list">

## 16: UNAUTHENTICATED {#unauthenticated}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрос не содержит действительных учётных данных для аутентификации.

Повторите запрос с актуальными данными для аутентификации.

[*instant]: {% include [instant](./_includes/tooltips/instant.md) %}

[*fastbackoff]: {% include [fast backoff](./_includes/tooltips/fast_backoff.md) %}

[*slowbackoff]: {% include [slow backoff](./_includes/tooltips/slow_backoff.md) %}

[*retryable]: {% include [retryable](./_includes/tooltips/retryable.md) %}

[*nonretryable]: {% include [nonretryable](./_includes/tooltips/nonretryable.md) %}

[*condretryable]: {% include [conditionally retryable](./_includes/tooltips/condretryable.md) %}

# Статусы завершения сервера {{ ydb-short-name }}

[//]: # (Information from https://GitHub.com/ydb-platform/ydb-go-sdk/blob/master/retry/errors_data_test.go)

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
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400020](#unauthorized)
|
[UNAUTHORIZED](#unauthorized)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400030](#internal-error)
|
[INTERNAL_ERROR](#internal-error)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400040](#aborted)
|
[ABORTED](#aborted)
|
[повторяемый](*retryable)
|
[короткая](*fastbackoff)
|
нет
||

||
[400050](#unavailable)
|
[UNAVAILABLE](#unavailable)
|
[повторяемый](*retryable)
|
[короткая](*fastbackoff)
|
нет
||

||
[400060](#overloaded)
|
[OVERLOADED](#overloaded)
|
[повторяемый](*retryable)
|
[большая](*slowbackoff)
|
нет
||

||
[400070](#scheme-error)
|
[SCHEME_ERROR](#scheme-error)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400080](#generic-error)
|
[GENERIC_ERROR](#generic-error)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400090](#timeout)
|
[TIMEOUT](#timeout)
|
[неповторяемый](*nonretryable)
<!-- conditionally-retryable -->
<!-- TODO: Why is it non-retryable ? -->
|
–
|
нет
||

||
[400100](#bad-session)
|
[BAD_SESSION](#bad-session)
|
[повторяемый](*retryable)
|
[моментально](*instant)
|
да
||

||
[400120](#precondition-failed)
|
[PRECONDITION_FAILED](#precondition-failed)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400130](#already-exists)
|
[ALREADY_EXISTS](#already-exists)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400140](#not-found)
|
[NOT_FOUND](#not-found)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400150](#session-expired)
|
[SESSION_EXPIRED](#session-expired)
|
[повторяемый](*retryable)
|
[моментально](*instant)
|
да
||

||
[400160](#cancelled)
|
[CANCELLED](#cancelled)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400170](#undetermined)
|
[UNDETERMINED](#undetermined)
|
[условно повторяемый](*condretryable)
|
[короткая](*fastbackoff)
|
нет
||

||
[400180](#unsupported)
|
[UNSUPPORTED](#unsupported)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||

||
[400190](#session-busy)
|
[SESSION_BUSY](#session-busy)
|
[повторяемый](*retryable)
|
[короткая](*fastbackoff)
|
да
||

||
[400200](#external-error)
|
[EXTERNAL_ERROR](#external-error)
|
[неповторяемый](*nonretryable)
|
–
|
нет
||
|#

## 400000: SUCCESS {#success}

Не является ошибкой — запрос успешно обработан.

<div class="tags_list">

## 400010: BAD_REQUEST {#bad-request}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Ошибка в синтаксисе запроса, пропущены обязательные поля.

Проверьте запрос.

<div class="tags_list">

## 400020: UNAUTHORIZED {#unauthorized}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Отсутствует доступ к запрашиваемому схемному объекту (таблица, директория).

Запросите доступ у администратора базы данных.

<div class="tags_list">

## 400030: INTERNAL_ERROR {#internal-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Неизвестная внутренняя ошибка.

Зарегистрируйте проблему на [GitHub](https://github.com/ydb-platform/ydb/issues/new) или обратитесь к технической поддержке {{ ydb-short-name }}.

<div class="tags_list">

## 400040: ABORTED {#aborted}

{% include notitle [retryable-fast](./_includes/tags.md#retryable-fastbackoff) %}

</div>

Операция не выполнена (например, по причине инвалидации локов, `TRANSACTION_LOCKS_INVALIDATED` в подробных сообщениях об ошибке).

Повторите всю транзакцию.

<div class="tags_list">

## 400050: UNAVAILABLE {#unavailable}

{% include notitle [retryable-fastbackoff](./_includes/tags.md#retryable-fastbackoff) %}

</div>

Часть системы недоступна.

Повторите последнее действие (запрос).

<div class="tags_list">

## 400060: OVERLOADED {#overloaded}

{% include notitle [retryable-slowbackoff](./_includes/tags.md#retryable-slowbackoff) %}

</div>

Часть системы перегружена.

Повторите последнее действие (запрос), снизьте интенсивность потока запросов.

<div class="tags_list">

## 400070: SCHEME_ERROR {#scheme-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрос не соответствует схеме.

Исправьте запрос или схему.

<div class="tags_list">

## 400080: GENERIC_ERROR {#generic-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Неклассифицируемая ошибка, возможно, связанная с запросом.

Ознакомьтесь с подробным сообщением об ошибке. При необходимости зарегистрируйте проблему на [GitHub](https://github.com/ydb-platform/ydb/issues/new) или обратитесь к технической поддержке {{ ydb-short-name }}.

<div class="tags_list">

## 400090: TIMEOUT {#timeout}

{% include notitle [conditionally-retryable](./_includes/tags.md#conditionally-retryable) %}

</div>

Запрос не выполнен за отведённое время.

Можно повторить для идемпотентных запросов.

<div class="tags_list">

## 400100: BAD_SESSION {#bad-session}

{% include notitle [retryable-instant](./_includes/tags.md#retryable) %}

</div>

Данная сессия больше недоступна.

Пересоздайте сессию.

<div class="tags_list">

## 400120: PRECONDITION_FAILED {#precondition-failed}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрос не может быть выполнен в текущем состоянии (например, вставка в таблицу с существующим ключом).

Исправьте состояние или запрос и повторите попытку.

<div class="tags_list">

## 400130: ALREADY_EXISTS {#already-exists}

[//]: # (TODO: Verify the description)
{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Объект базы данных, который создаётся, уже существует в кластере {{ ydb-short-name }}.

Ответ зависит от логики приложения.

<div class="tags_list">

## 400140: NOT_FOUND {#not-found}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Объект базы данных не найден в {{ ydb-short-name }}.

Ответ зависит от логики приложения.

<div class="tags_list">

## 400150: SESSION_EXPIRED {#session-expired}

{% include notitle [conditionally-retryable-instant](./_includes/tags.md#conditionally-retryable) %}

</div>

Срок действия сессии уже истёк.

Пересоздать сессию.

<div class="tags_list">

## 400160: CANCELLED {#cancelled}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрос был отменён на сервере. Например, пользователь отменил запрос во [встроенном UI](../embedded-ui/index.md), который выполнялся слишком долго, или запрос был сделан с опцией таймаута [cancel_after](../../dev/timeouts.md#cancel).

Если выполнение запроса заняло слишком много времени, попробуйте оптимизировать запрос. Если использовалась опция таймаута `cancel_after`, увеличьте её значение.

<div class="tags_list">

## 400170: UNDETERMINED {#undetermined}

{% include notitle [conditionally-retryable-fastbackoff](./_includes/tags.md#conditionally-retryable-fastbackoff) %}

</div>

Состояние транзакции неизвестно. В результате выполнения запроса произошёл сбой, из-за которого невозможно определить состояние транзакции. На запросы, завершившиеся с таким статусом, распространяются гарантии целостности и атомарности транзакции. То есть либо все изменения зафиксированы, либо вся транзакция отменена.

Для идемпотентных транзакций можно повторить всю транзакцию с небольшой задержкой. В противном случае реакция зависит от логики приложения.

<div class="tags_list">

## 400180: UNSUPPORTED {#unsupported}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Запрос не поддерживается {{ ydb-short-name }}, либо потому, что обработка таких запросов ещё не реализована в данной версии {{ ydb-short-name }}, либо потому, что поддержка таких запросов не включена в конфигурации {{ ydb-short-name }}.

Исправьте запрос или включите поддержку подобных запросов в конфигурации {{ ydb-short-name }}.

<div class="tags_list">

## 400190: SESSION_BUSY {#session-busy}

{% include notitle [retryable-fastbackoff](./_includes/tags.md#retryable-fastbackoff) %}

</div>

Сессия занята.

Пересоздайте сессию.

<div class="tags_list">

## 400200: EXTERNAL_ERROR {#external-error}

{% include notitle [non-retryable](./_includes/tags.md#non-retryable) %}

</div>

Произошла ошибка во внешней системе, например, при обработке федеративного запроса или при импорте данных из внешнего источника.

Проанализируйте подробное сообщение об ошибке. При необходимости зарегистрируйте проблему на [GitHub](https://github.com/ydb-platform/ydb/issues/new) или обратитесь к технической поддержке {{ ydb-short-name }}.

## Смотрите также

[{#T}](../../faq/errors.md)


[*instant]: {% include [instant](./_includes/tooltips/instant.md) %}

[*fastbackoff]: {% include [fast backoff](./_includes/tooltips/fast_backoff.md) %}

[*slowbackoff]: {% include [slow backoff](./_includes/tooltips/slow_backoff.md) %}

[*retryable]: {% include [retryable](./_includes/tooltips/retryable.md) %}

[*nonretryable]: {% include [nonretryable](./_includes/tooltips/nonretryable.md) %}

[*condretryable]: {% include [conditionally retryable](./_includes/tooltips/condretryable.md) %}

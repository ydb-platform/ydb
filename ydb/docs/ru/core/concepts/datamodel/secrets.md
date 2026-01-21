# Секреты

Для аутентификации во внешних системах используются различные реквизиты доступа. Реквизиты доступа хранятся в отдельных объектах – секретах. Секреты доступны только для записи и обновления, получить значение секрета нельзя.
В {{ ydb-full-name }} секреты используются, например, в [федеративных запросах](../federated_query/index.md) и [трансферах данных](../transfer.md).

## Синтаксис {#syntax}

* Создание секрета выполняется с помощью  [CREATE SECRET](../../yql/reference/syntax/create-secret.md)
* Изменение секрета выполняется с помощью  [ALTER SECRET](../../yql/reference/syntax/alter-secret.md).
* Удаление секрета выполняется с помощью  [DROP SECRET](../../yql/reference/syntax/drop-secret.md).

## Использование {#secret-usage}

Примеры использования секретов и работа с ними есть в следующих разделах:
* [{#T}](../../yql/reference/recipes/ttl.md) 
* [{#T}](../../recipes/import-export-column-tables.md)

## Управление доступом {#secret_access}

Секреты являются объектами схемы, поэтому права на них выдаются с помощью [команды](../../yql/reference/syntax/grant.md) `GRANT`, а отзываются – с помощью [команды](../../yql/reference/syntax/revoke.md) `REVOKE`. Для использования секрета в запросе, например, при создании [внешнего источника данных](../../yql/reference/syntax/create-external-data-source.md) или [трансфера данных](../../yql/reference/syntax/create-transfer.md), необходимо [право](../../yql/reference/syntax/grant.md#permissions-list) `SELECT ROW`.

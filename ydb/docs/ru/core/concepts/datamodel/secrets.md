# Секреты

Для аутентификации во внешних системах используются различные реквизиты доступа. Реквизиты доступа хранятся в отдельных объектах – секретах. Секреты доступны только для записи и обновления, получить значение секрета нельзя.
В {{ ydb-full-name }} секреты используются, например, в [федеративных запросах](../federated_query/index.md) и [трансферах данных](../transfer.md).

## Создание секретов {#create_secret}

Создание секрета выполняется с помощью SQL-запроса:

```yql
CREATE SECRET MySecretName WITH (value = "MySecretData");
```

Подробнее о синтаксисе команды см. в разделе [CREATE SECRET](../../yql/reference/syntax/create-secret.md).

## Управление доступом {#secret_access}

Секреты являются объектами схемы, поэтому права на них выдаются с помощью [команды](../../yql/reference/syntax/grant.md) `GRANT`, а отзываются – с помощью [команды](../../yql/reference/syntax/revoke.md) `REVOKE`. Для использования секрета в запросе, например, при создании [внешнего источника данных](../../yql/reference/syntax/create-external-data-source.md) или [трансфера данных](../../yql/reference/syntax/create-transfer.md), необходимо [право](../../yql/reference/syntax/grant.md#permissions-list) `SELECT ROW`.

## Устаревший синтаксис

{% note warning %}

**Команды, указанные ниже, устарели** и будут удалены в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с секретами описан выше.

{% endnote %}

### Создание секретов {#create_secret_deprecated}

Устаревший синтаксис создания секретов описан в разделе  [CREATE OBJECT (TYPE SECRET)](../../yql/reference/syntax/create-object-type-secret.md).

### Управление доступом {#secret_access_deprecated}

Устаревший синтаксис выдачи разрешения на использование секрета описан в разделе  [CREATE OBJECT (TYPE SECRET_ACCESS)](../../yql/reference/syntax/create-object-type-secret-access.md).

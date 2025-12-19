# Секреты

Для аутентификации во внешних системах используются различные реквизиты доступа. Реквизиты доступа хранятся в отдельных объектах – секретах. Секреты доступны только для записи и обновления, получить значение секрета нельзя.
В {{ ydb-full-name }} секреты используются, например, в [федеративных запросах](../federated_query/index.md) и [трансферах данных](../transfer.md).

## Создание секретов {#create_secret}

Создание секрета выполняется с помощью SQL-запроса:

```yql
CREATE SECRET `MySecretName` WITH (value = `MySecretData`);
```

Подробнее о синтаксисе команды см. в разделе [CREATE SECRET](../../yql/reference/syntax/create-secret.md).

## Управление доступом {#secret_access}

Секреты являются объектами схемы, поэтому права на них выдаются с помощью [команды](../../yql/reference/syntax/grant.md) `GRANT`, а отзываются – с помощью [команды](../../yql/reference/syntax/revoke.md) `REVOKE`. Для использования секрета в запросе, например, при создании [внешнего источника данных](../../yql/reference/syntax/create-external-data-source.md) или [трансфера данных](../../yql/reference/syntax/create-transfer.md), необходимо [право](../../yql/reference/syntax/grant.md) `ydb.granular.select_row`.

## Устаревший синтаксис

### Создание секретов {#create_secret_depricated}

{% note warning %}

**Данная команда устарела** и будет удалена в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с секретами описан в разделе [Создание секретов](#create_secret).

{% endnote %}

Создание секрета выполняется с помощью SQL-запроса:

```yql
CREATE OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;
```

### Управление доступом {#secret_access_depricated}

{% note warning %}

**Данная команда устарела** и будет удалена в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с доступами к секретам описан в разделе [Управление доступом](#secret_access).

{% endnote %}

Все права на использование секрета принадлежат создателю секрета. Создатель секрета может предоставить право чтения секрета другому пользователю с помощью [управления доступом](#secret_access) к секретам.

Для управления доступами к секретам используются специальные объекты `SECRET_ACCESS`. Для выдачи разрешения на использование секрета `MySecretName` пользователю `another_user` необходимо создать объект `SECRET_ACCESS` с именем `MySecretName:another_user`.

```yql
CREATE OBJECT `MySecretName:another_user` (TYPE SECRET_ACCESS)
```

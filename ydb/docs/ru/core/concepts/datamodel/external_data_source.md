# Внешние источники данных

{% note warning %}

Данная функциональность находится в режиме "Experimental".

{% endnote %}


Внешний источник (external data source) - это объект в {{ ydb-full-name }}, описывающий параметры подключения к внешнему источнику данных. Например, в случае ClickHouse внешний источник описывает сетевой адрес, логин и пароль для аутентификации в кластере ClickHouse, а в случае S3 ({{ objstorage-name }}) описывает реквизиты доступа и путь к бакету.

В следующем примере приведен пример создания внешнего источника, ведущего на кластер ClickHouse:

```sql
CREATE EXTERNAL DATA SOURCE test_data_source WITH (
  SOURCE_TYPE="ClickHouse",
  LOCATION="192.168.1.1:8123",
  DATABASE_NAME="default",
  AUTH_METHOD="BASIC",
  USE_TLS="TRUE",
  LOGIN="login",
  PASSWORD_SECRET_NAME="test_password_name"
)
```

После создания внешнего источника данных можно выполнять чтение данных из созданного объекта `EXTERNAL DATA SOURCE`. Пример ниже иллюстрирует чтение данных из таблицы `test_table` из базы данных `default` в кластере ClickHouse:

```sql
SELECT * FROM test_data_source.test_table
```

С помощью внешних источников данных можно выполнять [федеративные запросы](../federated_query/index.md) для задач межсистемной аналитики данных.

В качестве источников данных можно использовать:
- [ClickHouse](../federated_query/clickhouse.md)
- [PostgreSQL](../federated_query/postgresql.md)
- [Подключения к S3 ({{ objstorage-name }})](../federated_query/s3/external_data_source.md)


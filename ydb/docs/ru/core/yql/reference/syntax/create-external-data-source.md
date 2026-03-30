# CREATE EXTERNAL DATA SOURCE

<!-- markdownlint-disable proper-names -->

Вызов `CREATE EXTERNAL DATA SOURCE` создает [внешний источник данных](../../../concepts/datamodel/external_data_source.md).

```yql
CREATE EXTERNAL DATA SOURCE external_data_source WITH (
  SOURCE_TYPE="source_type",
  LOCATION="ip_address_or_fqdn:port",
  USE_TLS="use_tls",
  AUTH_METHOD="auth_method",
  LOGIN="login",
  PASSWORD_SECRET_PATH="password_secret_path",
  AWS_ACCESS_KEY_ID_SECRET_PATH="aws_access_key_id_secret_path",
  AWS_SECRET_ACCESS_KEY_SECRET_PATH="aws_secret_access_key_secret_path",
  AWS_ACCESS_KEY_ID_SECRET_NAME="aws_access_key_id_secret_name",
  AWS_SECRET_ACCESS_KEY_SECRET_NAME="aws_secret_access_key_secret_name",
  AWS_REGION="aws_region"
)
```

Где:

* `external_data_source` - название внешнего источника данных.
* `source_type` - тип внешнего источника данных. Возможные значения: [`ClickHouse`](#clickhouse), [`PostgreSQL`](#postgresql), [`ObjectStorage`](#object_storage).
* `ip_address_or_fqdn:port` - полный сетевой адрес внешнего источника данных, включая порт. В качестве сетевого адреса можно указывать IP-адрес или FQDN.
* `use_tls` - флаг, указывающий требование подключения через безопасное соединение (TLS). Возможные значения: `TRUE`, `FALSE`.
* `auth_method` - способ аутентификации во внешнем источнике данных. Для внешних источников типов `ClickHouse`, `PostgreSQL` поддерживается только тип аутентификации `BASIC`. Для внешнего источника `ObjectStorage` поддерживаются типы аутентификации `NONE` и `AWS`.
* `login` - логин, используемый для подключения к внешнему источнику данных.
* `password_secret_path` - [секрет](../../../concepts/datamodel/secrets.md), содержащий пароль для подключения к внешнему источнику данных.
* `aws_access_key_id_secret_path` / `aws_secret_access_key_secret_path` - путь к [секрету](../../../concepts/datamodel/secrets.md), содержащему `Access Key ID` / `Secret Access Key` для `AUTH_METHOD="AWS"`.
* `aws_access_key_id_secret_name` / `aws_secret_access_key_secret_name` - имя схемного объекта [секрета](../../../concepts/datamodel/secrets.md) (полный путь), содержащего `Access Key ID` / `Secret Access Key` для `AUTH_METHOD="AWS"`. Удобно использовать для интеграции с tiering в `ColumnShard`.
* `aws_region` - регион S3-совместимого хранилища для `AUTH_METHOD="AWS"`.

При работе по защищенным TLS каналам связи используется системные сертификаты, расположенные на серверах {{ydb-full-name}}.

## Пример

Запрос ниже создает внешний источник с именем `TestDataSource` к кластеру ClickHouse c IP-адресом `192.168.1.1` и портом `8443`, логином `admin` и секретом `test_secret`:

```yql
CREATE EXTERNAL DATA SOURCE TestDataSource WITH (
  SOURCE_TYPE="ClickHouse",
  LOCATION="192.168.1.1:8443",
  USE_TLS="TRUE",
  AUTH_METHOD="BASIC",
  LOGIN="admin",
  PASSWORD_SECRET_PATH="test_secret"
)
```

## Подключение к ClickHouse {#clickhouse}

Для создания подключения к кластеру ClickHouse необходимо создать внешний источник данных `EXTERNAL DATA SOURCE`, указав:

- В поле `SOURCE_TYPE` значение `ClickHouse`.
- В поле `LOCATION` полный сетевой адрес кластера ClickHouse, включая порт. В качестве сетевого адреса можно указывать IP-адрес или FQDN. В данный момент подключение к кластеру ClickHouse всегда выполняется про HTTP протоколу.
- В поле `USE_TLS` флаг, указывающий требование подключения к кластеру ClickHouse через безопасное соединение (TLS).
- В поле `AUTH_METHOD` значение `BASIC`.
- В поле `LOGIN` логин, используемый для подключения к кластеру ClickHouse.
- В поле `PASSWORD_SECRET_PATH` [секрет](../../../concepts/datamodel/secrets.md), содержащий пароль для подключения к кластеру ClickHouse.

При работе по защищенным TLS каналам связи используется системные сертификаты, расположенные на серверах {{ ydb-full-name }}.

### Пример

Запрос ниже создает внешний источник с именем `TestDataSource` к кластеру ClickHouse c IP-адресом `192.168.1.1` и портом `8443`, логином `admin` и секретом `test_secret`:

```yql
CREATE EXTERNAL DATA SOURCE TestDataSource WITH (
  SOURCE_TYPE="ClickHouse",
  LOCATION="192.168.1.1:8443",
  USE_TLS="TRUE",
  AUTH_METHOD="BASIC",
  LOGIN="admin",
  PASSWORD_SECRET_PATH="test_secret"
)
```

## Подключение к PostgreSQL {#postgresql}

Для создания подключения к кластеру PostgreSQL необходимо создать объект `EXTERNAL DATA SOURCE`, указав в полях:

- в поле `SOURCE_TYPE` значение `PostgreSQL`;
- в поле `LOCATION` полный сетевой адрес кластера PostgreSQL, включая порт. В качестве сетевого адреса можно указывать IP-адрес или FQDN;
- в поле `USE_TLS` флаг, указывающий требование подключения к кластеру PostgreSQL через безопасное соединение (TLS);
- в поле `AUTH_METHOD` значение `BASIC`;
- в поле `LOGIN` логин, используемый для подключения к кластеру PostgreSQL;
- в поле `PASSWORD_SECRET_PATH` [секрет](../../../concepts/datamodel/secrets.md), содержащий пароль для подключения к кластеру PostgreSQL.

В данный момент подключение к кластеру PostgreSQL всегда выполняется про стандартному ([Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)) по транспорту TCP. При работе по защищенным TLS каналам связи используется системные сертификаты, расположенные на серверах {{ydb-full-name}}.

### Пример

Запрос ниже создает внешний источник с именем `TestDataSource`, ведущий на кластер PostgreSQL c IP-адресом `192.168.1.2` и портом `5432`, логином `admin` и секретом `test_secret`:

```yql
CREATE EXTERNAL DATA SOURCE TestDataSource WITH (
  SOURCE_TYPE="PostgreSQL",
  LOCATION="192.168.1.1:5432",
  USE_TLS="TRUE",
  AUTH_METHOD="BASIC",
  LOGIN="admin",
  PASSWORD_SECRET_PATH="test_secret"
)
```

## Подключение к S3 ({{ objstorage-name }}) {#object_storage}

Для создания внешнего источника данных, ведущего на бакет с данными в S3 ({{ objstorage-name }}) необходимо создать объект `EXTERNAL DATA SOURCE`, указав в полях:

- в поле `SOURCE_TYPE` значение `ObjectStorage`;
- в поле `LOCATION` сетевой путь к бакету;
- в поле `AUTH_METHOD` одно из значений:
  - `NONE` — для публичного бакета без аутентификации;
  - `AWS` — для доступа с AWS-учетными данными.

Если используется `AUTH_METHOD="AWS"`, укажите параметры:

- `AWS_ACCESS_KEY_ID_SECRET_PATH` и `AWS_SECRET_ACCESS_KEY_SECRET_PATH`, либо
- `AWS_ACCESS_KEY_ID_SECRET_NAME` и `AWS_SECRET_ACCESS_KEY_SECRET_NAME`.

Параметры с суффиксом `_SECRET_NAME` ссылаются на схемные объекты секретов и рекомендуются для сценариев с `ColumnShard` tiering.

Также для `AUTH_METHOD="AWS"` необходимо указать `AWS_REGION`.

Подключение возможно к любым источникам данных с протоколом доступа [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html). Возможно указание любых URL к системам, поддерживающим этот протокол.

Типовые значения поля `LOCATION` при подключении к различным системам к бакету `bucket`:

|Название системы|URL|
|------|-------|
|{{ objstorage-name }}|`https://storage.yandexcloud.net/bucket/`|
|AWS S3|`http://s3.amazonaws.com/bucket/`|

### Пример

Запрос ниже создает внешний источник данных с именем `TestDataSource`, ведущий в каталог `folder` в бакете `bucket` в {{ objstorage-name }}:

```yql
CREATE EXTERNAL DATA SOURCE TestDataSource WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="http://s3.amazonaws.com/bucket/folder/",
  AUTH_METHOD="NONE"
)
```

Пример для приватного S3-совместимого хранилища с аутентификацией `AWS` и ссылками на схемные объекты секретов:

```yql
CREATE EXTERNAL DATA SOURCE TestPrivateDataSource WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="http://s3.amazonaws.com/bucket/folder/",
  AUTH_METHOD="AWS",
  AWS_ACCESS_KEY_ID_SECRET_NAME="/Root/access-key-secret",
  AWS_SECRET_ACCESS_KEY_SECRET_NAME="/Root/secret-key-secret",
  AWS_REGION="ru-central-1"
)
```


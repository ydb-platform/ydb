# CREATE EXTERNAL DATA SOURCE

<!-- markdownlint-disable proper-names -->

The `CREATE EXTERNAL DATA SOURCE` statement creates an [external data source](../../../concepts/datamodel/external_data_source.md).

```yql
CREATE EXTERNAL DATA SOURCE external_data_source WITH (
  SOURCE_TYPE="source_type",
  LOCATION="ip_address_or_fqdn:port",
  USE_TLS="use_tls",
  AUTH_METHOD="auth_method",
  LOGIN="login",
  PASSWORD_SECRET_PATH="password_secret_path"
)
```

Where:

* `external_data_source` — name of the external data source.
* `source_type` — type of external data source. Allowed values: [`ClickHouse`](#clickhouse), [`PostgreSQL`](#postgresql), [`ObjectStorage`](#object_storage).
* `ip_address_or_fqdn:port` — full network address of the external data source, including port. The address may be an IP or FQDN.
* `use_tls` — flag requiring a secure (TLS) connection. Allowed values: `TRUE`, `FALSE`.
* `auth_method` — authentication method for the external source. For `ClickHouse` and `PostgreSQL` sources, only `BASIC` is supported. For `ObjectStorage`, only `NONE` is supported at this time.
* `login` — login used to connect to the external data source.
* `password_secret_path` — [secret](../../../concepts/datamodel/secrets.md) that stores the password for the external data source.

For TLS, the system uses certificates installed on {{ ydb-full-name }} servers.

## Example

The following creates an external data source named `TestDataSource` for a ClickHouse cluster at `192.168.1.1:8443`, with login `admin` and secret `test_secret`:

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

## Connecting to ClickHouse {#clickhouse}

To connect to a ClickHouse cluster, create an `EXTERNAL DATA SOURCE` and set:

- `SOURCE_TYPE` to `ClickHouse`.
- `LOCATION` to the full network address of the ClickHouse cluster, including port (IP or FQDN). Connections currently always use the HTTP protocol.
- `USE_TLS` to indicate whether TLS is required.
- `AUTH_METHOD` to `BASIC`.
- `LOGIN` to the ClickHouse login.
- `PASSWORD_SECRET_PATH` to a [secret](../../../concepts/datamodel/secrets.md) with the ClickHouse password.

For TLS, the system uses certificates installed on {{ ydb-full-name }} servers.

### Example

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

## Connecting to PostgreSQL {#postgresql}

To connect to a PostgreSQL cluster, create an `EXTERNAL DATA SOURCE` with:

- `SOURCE_TYPE` set to `PostgreSQL`;
- `LOCATION` set to the full address of the PostgreSQL cluster, including port (IP or FQDN);
- `USE_TLS` set according to whether TLS is required;
- `AUTH_METHOD` set to `BASIC`;
- `LOGIN` set to the PostgreSQL login;
- `PASSWORD_SECRET_PATH` set to a [secret](../../../concepts/datamodel/secrets.md) with the PostgreSQL password.

Connections use the standard [Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html) over TCP. For TLS, the system uses certificates installed on {{ ydb-full-name }} servers.

### Example

The following creates an external data source named `TestDataSource` for a PostgreSQL cluster at `192.168.1.2:5432`, with login `admin` and secret `test_secret`:

```yql
CREATE EXTERNAL DATA SOURCE TestDataSource WITH (
  SOURCE_TYPE="PostgreSQL",
  LOCATION="192.168.1.2:5432",
  USE_TLS="TRUE",
  AUTH_METHOD="BASIC",
  LOGIN="admin",
  PASSWORD_SECRET_PATH="test_secret"
)
```

## Connecting to S3 ({{ objstorage-name }}) {#object_storage}

To create an external data source for a data bucket in S3 ({{ objstorage-name }}), create an `EXTERNAL DATA SOURCE` with:

- `SOURCE_TYPE` set to `ObjectStorage`;
- `LOCATION` set to the network path to the bucket;
- `AUTH_METHOD` set to `NONE`.

{% note info %}

Only buckets that do not require authentication are supported at this time.

{% endnote %}

You can connect to any data source that exposes an [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)-compatible API, including custom URLs for S3-compatible systems.

Typical `LOCATION` values for bucket `bucket`:

| System | URL |
|------|-------|
| {{ objstorage-name }} | `https://storage.yandexcloud.net/bucket/` |
| AWS S3 | `http://s3.amazonaws.com/bucket/` |

### Example

The following creates an external data source named `TestDataSource` pointing to folder `folder` in bucket `bucket` in {{ objstorage-name }}:

```yql
CREATE EXTERNAL DATA SOURCE TestDataSource WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="http://s3.amazonaws.com/bucket/folder/",
  AUTH_METHOD="NONE"
)
```

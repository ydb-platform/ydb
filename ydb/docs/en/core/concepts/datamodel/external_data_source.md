# External data sources

An external data source is an object in {{ ydb-full-name }} that describes connection parameters to an external data source. For example, in the case of ClickHouse, the external data source describes the network address, login, and password for authentication in the ClickHouse cluster, and in the case of S3 ({{ objstorage-name }}), it describes access credentials and the path to the bucket.

The following example shows how to create an external data source pointing to a ClickHouse cluster:


```yql
CREATE EXTERNAL DATA SOURCE test_data_source WITH (
  SOURCE_TYPE="ClickHouse",
  LOCATION="192.168.1.1:8123",
  DATABASE_NAME="default",
  AUTH_METHOD="BASIC",
  USE_TLS="TRUE",
  LOGIN="login",
  PASSWORD_SECRET_PATH="test_password_path",
  PROTOCOL="NATIVE"
);
```


After creating an external data source, you can read data from the created `EXTERNAL DATA SOURCE` object. The example below illustrates reading data from the `test_table` table in the `default` database in the ClickHouse cluster:


```yql
SELECT * FROM test_data_source.test_table;
```


Using external data sources, you can run [federated queries](../query_execution/federated_query/index.md) for cross-system data analytics tasks.

You can use the following data sources:

{% include [!](../query_execution/federated_query/_includes/supported_eds.md) %}

{% cut "Experimental sources data" %}

{% include [!](../query_execution/federated_query/_includes/experimental_eds.md) %}

{% endcut %}

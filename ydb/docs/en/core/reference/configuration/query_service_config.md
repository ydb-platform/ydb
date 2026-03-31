# query_service_config

<<<<<<< HEAD
The `query_service_config` section describes the parameters for {{ ydb-short-name }} to work with external data sources using [federated queries](../../concepts/query_execution/federated_query/index.md).

If access to the required source requires deploying a [connector](../../concepts/query_execution/federated_query/architecture.md#connectors), it must also be configured according to the [instructions](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).
=======
The `query_service_config` section describes the parameters for {{ ydb-short-name }} to work with external data sources using [federated queries](../../concepts/federated_query/index.md).

If access to the required source requires deploying a [connector](../../concepts/federated_query/architecture.md#connectors), it must also be configured according to the [instructions](../../devops/deployment-options/manual/federated-queries/connector-deployment.md).
>>>>>>> 72b0633e8f8 (DOCORG125171-Translate (#35672))

## Parameter description

#|
|| **Parameter** | **Default value** | **Description** ||
|| `generic.connector.endpoint.host`
| `localhost`
| Connector host name.
||
|| `generic.connector.endpoint.port`
| `2130`
| Connector TCP port.
||
|| `generic.connector.use_ssl`
| `false`
| Whether to use connection encryption. When the connector and {{ ydb-short-name }} dynamic node are deployed on the same server, encrypted connection between them is not required, but it can be enabled if needed.
||
|| `generic.connector.ssl_ca_crt`
| empty string
| Path to the CA certificate used for encryption.
||
|| `generic.default_settings.name.UsePredicatePushdown`
| `false`
| Enables predicate pushdown to external data sources: some parts of SQL queries (for example, filters) will be passed for execution to the external source. This can significantly reduce the volume of data transferred over the network from the data source to federated {{ ydb-short-name }}, save its computational resources, and significantly reduce federated query processing time.
||
|| `available_external_data_sources`
| empty list
| List of allowed external data source types. Applied when `all_external_data_sources_are_available: false`.

Possible values:

* `ObjectStorage`;
* `ClickHouse`;
* `PostgreSQL`;
* `MySQL`;
* `Greenplum`;
* `MsSQLServer`;
* `Ydb`.

||
|| `all_external_data_sources_are_available`
| `false`
| Enable all external data source types. When enabled, the `available_external_data_sources` setting is ignored.
||
|#

## Examples {#examples}

### Enabling ClickHouse and MySQL external sources

```yaml
query_service_config:
  generic:
    connector:
      endpoint:
        host: localhost                   # host name where the connector is deployed
        port: 2130                        # connector port number
      use_ssl: false                      # flag to enable connection encryption
      ssl_ca_crt: "/opt/ydb/certs/ca.crt" # path to CA certificate
    default_settings:
    - name: UsePredicatePushdown
      value: "true"
  all_external_data_sources_are_available: false
  available_external_data_sources:
  - ClickHouse
  - MySQL
```

### Enabling all external data source types

```yaml
query_service_config:
  generic:
    connector:
      endpoint:
        host: localhost                   # host name where the connector is deployed
        port: 2130                        # connector port number
      use_ssl: false                      # flag to enable connection encryption
      ssl_ca_crt: "/opt/ydb/certs/ca.crt" # path to CA certificate
    default_settings:
    - name: UsePredicatePushdown
      value: "true"
  all_external_data_sources_are_available: true
```

## See also

- [{#T}](../../devops/deployment-options/manual/federated-queries/index.md)

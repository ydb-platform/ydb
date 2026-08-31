# tls

The `tls` section configures [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) settings for [data-in-transit encryption](../../security/encryption/data-in-transit.md) in {{ ydb-short-name }}. Each network protocol can have different TLS settings to secure communication between cluster components and clients.

## Interconnect {#interconnect}

The [{{ ydb-short-name }} actor system interconnect](../../concepts/glossary.md#actor-system-interconnect) is a specialized protocol for communication between {{ ydb-short-name }} nodes.

Example of enabling TLS for interconnect:


```yaml
interconnect_config:
   start_tcp: true
   encryption_mode: REQUIRED # or OPTIONAL
   path_to_certificate_file: "/opt/ydb/certs/node.crt"
   path_to_private_key_file: "/opt/ydb/certs/node.key"
   path_to_ca_file: "/opt/ydb/certs/ca.crt"
```


## {{ ydb-short-name }} as a server

### gRPC {#grpc}

The main [{{ ydb-short-name }} API](../../reference/ydb-sdk/overview-grpc-api.md) is based on [gRPC](https://grpc.io/). It is used for external communication with client applications that work natively with {{ ydb-short-name }} via the [SDK](../../reference/ydb-sdk/index.md) or [CLI](../../reference/ydb-cli/index.md).

Example of enabling TLS for gRPC API:


```yaml
grpc_config:
   cert: "/opt/ydb/certs/node.crt"
   key: "/opt/ydb/certs/node.key"
   ca: "/opt/ydb/certs/ca.crt"
```


### Kafka Wire Protocol

{{ ydb-short-name }} exposes a separate network port for the [Kafka wire protocol](../../reference/kafka-api/index.md). This protocol is used for external communication with client applications initially designed to work with [Apache Kafka](https://kafka.apache.org/).

Example of enabling TLS for the Kafka protocol using a file containing both the certificate and the private key:


```yaml
kafka_proxy_config:
    ssl_certificate: "/opt/ydb/certs/node.crt"
```


Example of enabling TLS for the Kafka protocol with separate certificate and private key files:


```yaml
kafka_proxy_config:
    cert: "/opt/ydb/certs/node.crt"
    key: "/opt/ydb/certs/node.key"
```


### HTTP

{{ ydb-short-name }} opens a separate HTTP port for the built-in interface, displaying [metrics](../../devops/observability/monitoring.md), and other auxiliary commands.

Example of enabling TLS on the HTTP port, making it use HTTPS:


```yaml
monitoring_config:
    monitoring_certificate_file: "/opt/ydb/certs/node.crt"
```


For a detailed description of TLS parameters for monitoring, see the [monitoring_config](./monitoring_config.md#tls) section.

## {{ ydb-short-name }} as a client

### LDAP

{{ ydb-short-name }} supports [LDAP](../../security/authentication.md#ldap) for user authentication. The LDAP protocol has two options for enabling TLS.

Example of enabling TLS for LDAP via the `StartTls` protocol extension:


```yaml
auth_config:
  ldap_authentication:
    use_tls:
      enable: true
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
  scheme: "ldap"
```


Example of enabling TLS for LDAP via `ldaps`:


```yaml
auth_config:
  ldap_authentication:
    use_tls:
      enable: false
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
  scheme: "ldaps"
```


For more details, see [{#T}](../../devops/configuration-management/configuration-v1/#ldap-auth-config).

### Federated Queries

[Federated queries](../../concepts/query_execution/federated_query/index.md) allow {{ ydb-short-name }} to execute queries against various external data sources. The use of TLS when executing such queries is controlled by the `USE_TLS` parameter in [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md) statements. No changes to the server configuration are required.

### Tracing

{{ ydb-short-name }} can send [tracing](../../reference/observability/tracing/setup.md) data to an external collector via gRPC.

Example of enabling TLS for tracing data by specifying `grpcs://` protocol:


```yaml
tracing_config:
  backend:
    opentelemetry:
      collector_url: grpcs://example.com:4317
      service_name: ydb
```


{% if feature_async_replication %}

## Asynchronous Replication

[Asynchronous replication](../../concepts/async-replication.md) synchronizes data between two {{ ydb-short-name }} databases, where one serves as a client to the other. Whether this communication uses TLS-encrypted connections is controlled by the `CONNECTION_STRING` setting of [CREATE ASYNC REPLICATION](../../yql/reference/syntax/create-async-replication.md) queries. Use the `grpcs://` protocol for TLS connections. No changes to the server-side configuration are required.

When using a custom Certificate Authority (CA), pass its certificate in the `CA_CERT` parameter when creating an instance of asynchronous replication.

{% endif %}

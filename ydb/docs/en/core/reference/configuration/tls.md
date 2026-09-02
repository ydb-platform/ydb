# tls

The `tls` section configures [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) parameters for [encrypting data in transit over the network](../../security/encryption/data-in-transit.md) in {{ ydb-short-name }}. Each network protocol can have different TLS settings to ensure secure communication between cluster components and clients.

## Interconnect {#interconnect}

The [{{ ydb-short-name }} actor system interconnect](../../concepts/glossary.md#actor-system-interconnect) is a specialized protocol for exchanging data between {{ ydb-short-name }} nodes.

Example of enabling TLS for the interconnect:


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

The [main {{ ydb-short-name }} API](../../reference/ydb-sdk/overview-grpc-api.md) is based on [gRPC](https://grpc.io/). It is used for external interaction with client applications that work directly with {{ ydb-short-name }} via the [SDK](../../reference/ydb-sdk/index.md) or [CLI](../../reference/ydb-cli/index.md).

Example of enabling TLS for the gRPC API:


```yaml
grpc_config:
   cert: "/opt/ydb/certs/node.crt"
   key: "/opt/ydb/certs/node.key"
   ca: "/opt/ydb/certs/ca.crt"
```


### Kafka protocol

{{ ydb-short-name }} opens a separate network port for the [Kafka protocol](../../reference/kafka-api/index.md). This protocol is used for external interaction with client applications originally designed to work with [Apache Kafka](https://kafka.apache.org/).

Example of enabling TLS for the Kafka protocol using a file that contains both the certificate and the private key:


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

{{ ydb-short-name }} opens a separate HTTP port for [{{ ydb-ui-name }}](../../reference/ydb-ui/index.md), displaying [metrics](../../devops/observability/monitoring.md), and other auxiliary commands.

Example of enabling TLS on the HTTP port, which makes it use HTTPS:


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


This mechanism is described in more detail in [{#T}](../../devops/configuration-management/configuration-v1/#ldap-auth-config).

### Federated queries

[Federated queries](../../concepts/query_execution/federated_query/index.md) allow {{ ydb-short-name }} to run queries against various external data sources. The use of TLS when running such queries is controlled by the `USE_TLS` parameter in [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md) statements. No changes to the server configuration are required.

### Tracing

{{ ydb-short-name }} can send [tracing](../../reference/observability/tracing/setup.md) data to an external collector via gRPC.

Example of enabling TLS for tracing data by specifying the `grpcs://` protocol:


```yaml
tracing_config:
  backend:
    opentelemetry:
      collector_url: grpcs://example.com:4317
      service_name: ydb
```


{% if feature_async_replication %}

## Async replication

[Async replication](../../concepts/async-replication.md) synchronizes data between two {{ ydb-short-name }} databases, one of which acts as a client to the other. The use of TLS in such communication is controlled by the `CONNECTION_STRING` parameter in [CREATE ASYNC REPLICATION](../../yql/reference/syntax/create-async-replication.md) statements. For TLS connections, use the `grpcs://` protocol. No changes to the server configuration are required.

When using a custom Certificate Authority (CA), pass its certificate in the `CA_CERT` parameter when creating an asynchronous replication instance.

{% endif %}

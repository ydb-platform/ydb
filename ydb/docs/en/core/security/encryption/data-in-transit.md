# Data in transit encryption

As {{ ydb-short-name }} is a distributed system typically running on a cluster, often spanning multiple datacenters or availability zones, user data is routinely transferred over the network. Various protocols can be involved, and each can be configured to run over [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security). Below is a list of protocols supported by {{ ydb-short-name }}:

* [Interconnect](../../concepts/glossary.md#actor-system-interconnect), a specialized protocol for all communication between {{ ydb-short-name }} nodes.
* {{ ydb-short-name }} as a server:

  * [gRPC](../../reference/ydb-sdk/overview-grpc-api.md) for external communication with client applications designed to work natively with {{ ydb-short-name }} via the [SDK](../../reference/ydb-sdk/index.md) or [CLI](../../reference/ydb-cli/index.md).
  * [PostgreSQL wire protocol](../../postgresql/intro.md) for external communication with client applications initially designed to work with [PostgreSQL](https://www.postgresql.org/).
  * [Kafka wire protocol](../../reference/kafka-api/index.md) for external communication with client applications initially designed to work with [Apache Kafka](https://kafka.apache.org/).
  * HTTP for running the [Embedded UI](../../reference/embedded-ui/index.md), exposing [metrics](../../devops/observability/monitoring.md), and other miscellaneous endpoints.

* {{ ydb-short-name }} as a client:

  * [LDAP](../../security/authentication.md#ldap) for user authentication.
  * [Federated queries](../../concepts/federated_query/index.md), a feature that allows {{ ydb-short-name }} to query various external data sources. Some sources are queried directly from the `ydbd` process, while others are proxied via a separate connector process.
  * [Tracing](../../reference/observability/tracing/setup.md) data sent to an external collector via gRPC.

* In [asynchronous replication](../../concepts/async-replication.md) between two {{ ydb-short-name }} databases, one serves as a client to the other.

By default, data in transit encryption is disabled and must be enabled separately for each protocol. They can either share the same set of TLS certificates or use dedicated ones. For instructions on how to enable TLS, refer to the [{#T}](../../reference/configuration/tls.md) section.

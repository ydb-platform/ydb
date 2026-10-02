# Federated query processing system architecture

## External data sources and external tables

The key element of the {{ ydb-full-name }} federated query processing system is the concept of an [external data source](../../datamodel/external_data_source.md). Such sources can include relational DBMSs, object storages, and other data storage systems. When processing a federated query, {{ ydb-short-name }} streams data from external systems and allows performing the same range of operations on them as on local data.

To work with data stored in external systems, {{ ydb-short-name }} must have information about the internal structure of this data (for example, the number, names, and types of columns in tables). Some sources provide such metadata about the data together with the data itself, while working with other, unschematized sources requires specifying this metadata externally. [External tables](../../datamodel/external_table.md) serve this latter purpose.

After registering external data sources and (if necessary) external tables in {{ ydb-short-name }}, the client can start describing federated queries.

## Connectors {#connectors}

During the execution of federated queries, {{ ydb-short-name }} needs to access third-party data storage systems over the network, which requires using their client libraries. The introduction of such dependencies negatively affects the size of the codebase, compilation time, and the size of binary files {{ ydb-short-name }}, as well as the stability of the entire product.

The list of supported data sources for federated queries is constantly expanding.
The most popular sources, such as S3, are natively supported by {{ ydb-short-name }}. However, not all users need support for all sources at the same time. It can be enabled optionally using *connectors* - special microservices that implement a unified interface for accessing external data sources.

The functions of connectors include:

* Translation of YQL queries into queries in a language specific to the external source (for example, into queries in another SQL dialect or into HTTP API calls).
* Establishing network connections to data sources.
* Conversion of data extracted from external sources into a columnar representation in the [Arrow IPC Stream](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) format supported by {{ ydb-short-name }}.

![YDB Federated Query architecture](_assets/architecture.png "YDB Federated Query architecture" =640x)

Thus, connectors form an abstraction layer that hides the specifics of external data sources from {{ ydb-short-name }}. The conciseness of the connector interface makes it easy to expand the list of supported sources with minimal changes to the {{ ydb-short-name }} code.

Users can deploy [one of the ready-made connectors](../../../devops/deployment-options/manual/federated-queries/connector-deployment.md) or write their own implementation in any programming language according to the [gRPC specification](https://github.com/ydb-platform/ydb/tree/main/ydb/library/yql/providers/generic/connector/api).

## List of supported external data sources {#supported-datasources}

{{ ydb-full-name }} has built-in support in `ydbd` for the following external data sources:

{% include [!](_includes/supported_eds.md) %}

Through the [fq-connector-go](../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) connector, access to a number of experimental external DBMSs is additionally supported. For details, see the [{#T}](index.md) section.

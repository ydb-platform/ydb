* Added spans and traces implementation and OpenTelemetry demo 

* Added interface for export of metrics and spans, supported plugin for OpenTelemetry

* Supported gRPC compression option on client side

## v3.17.0

* Added support of describe for scheme objects with type 'secret' via new TSecretClient

* Added support for METRICS_LEVEL for the CreateTable/AlterTable requests.

* Added support for the new alter table compact action in TableClient and compaction operation in OperationClient in SDK

## v3.16.0

* Added support for the new inverted index type: JSON, intended to speed up queries on Json or JsonDocument columns.

* Fixed partition session id conflict

* Fixed hanging in PQ read session

## v3.15.0

* EXPERIMENTAL! Added `IProducer` interface to the SDK. This interface is used to write messages to a topic.
  Each message can be associated with a partitioning key, which is used to determine the partition to which the message will be written.

* Added gRPC load balancing policy option for `TDriver`. Default policy: `round_robin`.

* Added index_population_mode support in ImportFs

* Fixed a race condition in topic read session

* Fixed a bug where TTableClient destructor could hang indefinitely

## v3.14.0

* EXPERIMENTAL! Added direct read option for topic read session

* Added GetEndpoint for driver config

* Supported more connection string variants

* Removed fulltext index layout

* Supported TCP_NODELAY option for grpc sockets

* Supported access levels in whoami

* EXPERIMENTAL! Supported keyed write session for topics

* Supported PartitionMaxInFlightBytes for topic read session

## v3.13.0

* Removed the `layout` field from `FulltextIndexSettings` and replaced it with separate index types in `TableIndexDescription`.
  `layout` was a preliminary version of API, actual YDB release 26-1 uses separate index types, so please note that creating
  full text indexes via gRPC won't work with previous versions of SDK.

* Fixed infinite connect in blocking write topic session

* EXPERIMENTAL! Supported fulltext index

* Added deadline propagation

* Supported Message Level Parallelism in topics

* Supported ExportToFs and ImportFromFs

* Fixed bug with keep alive interval

* Added GetDatabase for driver

* Supported encryption settings for ExportFs

* Supported include index data for export and import

* Supported API for assigning vectors to multiple clusters in vector index

* Supported exclude_regexps in export/import API

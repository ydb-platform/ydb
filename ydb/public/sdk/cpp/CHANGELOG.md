* Added gRPC load balancing policy option for `TDriver`. Default policy: `round_robin`.

* Removed the `layout` field from `FulltextIndexSettings` and replaced it with separate index types in `TableIndexDescription`.
  `layout` was a preliminary version of API, actual YDB release 26-1 uses separate index types, so please note that creating
  full text indexes via gRPC won't work with previous versions of SDK.

* Added `IProducer` interface to the SDK. This interface is used to write messages to a topic.
Each message can be associated with a partitioning key, which is used to determine the partition to which the message will be written.

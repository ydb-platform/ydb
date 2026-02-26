* Removed the `layout` field from `FulltextIndexSettings` and replaced it with separate index types in `TableIndexDescription`.
  `layout` was a preliminary version of API, actual YDB release 26-1 uses separate index types, so please note that creating
  full text indexes via gRPC won't work with previous versions of SDK.

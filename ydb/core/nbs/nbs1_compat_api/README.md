# NBS1 compatibility API

This directory contains the protobuf schemas and C++ interface shared by the
NBS2 frontend and its NBS1-compatible transports. The original `cloud/...`
layout is retained; frontend and transport implementations live outside this
directory.

Copied types use `NYdb::NBS::NNbs1CompatApi` (and the corresponding protobuf
packages), so they can coexist with the original NBS1 types in one binary.
Imported macros retain their complete original name, adding only the prefix
`NBS1_COMPAT_`: for example, `BLOCKSTORE_SERVICE` becomes
`NBS1_COMPAT_SERVICE`.

The external gRPC service name remains
`NCloud.NBlockStore.NProto.TBlockStoreService`. The typed adapter
in `ydb/services/nbs/classic_grpc_service_adapter.cpp` registers its five
original RPC paths using the isolated message types. `service.proto` describes
the subset but does not generate gRPC stubs or a gateway: generating them from
the renamed package would change the RPC paths.

Keep protobuf field numbers, field encodings and enum values compatible with
NBS1. An original NBS1 client must work without knowing the internal package.

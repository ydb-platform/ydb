# gRPC API overview

{{ ydb-short-name }} provides the gRPC API, which you can use to manage your DB [resources](../../concepts/datamodel/index.md) and data. API methods and data structures are described using [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/proto3) (proto 3). For more information, see [.proto specifications with comments](https://github.com/ydb-platform/ydb-api-protos). Also {{ ydb-short-name }} uses special [gRPC metadata headers](grpc-headers.md).

The following services are available:

* [{#T}](health-check-api.md).

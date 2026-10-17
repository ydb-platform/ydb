LIBRARY()

SRCS(
    common.cpp
    mutation_actor.cpp
    query_actor.cpp
    rpc_udf.cpp
    table_query.cpp
)

PEERDIR(
    ydb/public/lib/udf/manifest
    library/cpp/digest/md5
    library/cpp/json
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_streaming
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/library/aclib
    ydb/library/services
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/resources
    ydb/services/metadata/request
    ydb/services/udf_store
    ydb/services/udf_store/compile_controller/protos
    ydb/services/udf_store/wasm
    ydb/services/udf_store/metadata_subscription
)

END()

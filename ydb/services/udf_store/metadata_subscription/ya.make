LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    udf_module.cpp
    snapshot.cpp
    wasm_artifact.cpp
    storage_paths.cpp
)

GENERATE_ENUM_SERIALIZATION(udf_module.h)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/accessor
    ydb/services/metadata
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

END()

LIBRARY()

SRCS(
    out.cpp
    scheme.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
)

END()

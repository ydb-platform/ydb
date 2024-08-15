LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    ydb_resources.cpp
    ydb_ca.cpp
)

RESOURCE(
    ydb/public/sdk/cpp/src/client/resources/ydb_sdk_version.txt ydb_sdk_version.txt
    ydb/public/sdk/cpp/src/client/resources/ydb_root_ca.pem ydb_root_ca.pem
)

END()

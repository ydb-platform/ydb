LIBRARY()

SRCS(
    ydb_resources.cpp
    ydb_ca.cpp
)

RESOURCE(
    ydb/public/sdk/cpp/src/client/resources/ydb_root_ca.pem ydb_root_ca_dev.pem
)

END()

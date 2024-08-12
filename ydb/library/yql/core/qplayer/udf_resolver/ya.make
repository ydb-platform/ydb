LIBRARY()

SRCS(
    yql_qplayer_udf_resolver.cpp
)

PEERDIR(
    ydb/library/yql/core/qplayer/storage/interface
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/core
    library/cpp/yson/node
    contrib/libs/openssl
)

END()


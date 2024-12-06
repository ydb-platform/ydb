LIBRARY()

SRCS(
    yql_qplayer_udf_resolver.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/providers/common/schema/expr
    yql/essentials/core
    library/cpp/yson/node
    contrib/libs/openssl
)

END()


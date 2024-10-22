LIBRARY()

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/blobs_action/tier
    ydb/core/tx/columnshard
    ydb/core/wrappers
)

SRCS(
    helper.cpp
    controllers.cpp
)

YQL_LAST_ABI_VERSION()

END()


LIBRARY()

SRCS(
    yson_helpers.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/providers/yt/common
)

END()

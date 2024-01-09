LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/yt/common
)

END()

LIBRARY()

SRCS(
    infer_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    ydb/library/yql/public/issue
    ydb/library/yql/utils/log
    ydb/library/yql/core/issue
)

END()

LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/schema/skiff
    ydb/library/yql/utils
)

END()

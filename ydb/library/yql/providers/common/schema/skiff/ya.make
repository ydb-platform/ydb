LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/public/udf
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/schema/parser
)

END()

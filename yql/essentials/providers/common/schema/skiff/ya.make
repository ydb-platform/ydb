LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/public/udf
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/parser
)

END()

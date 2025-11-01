LIBRARY()

SRCS(
    yql_mkql_schema.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/parser
    yql/essentials/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY(library-formats-arrow-minikql)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/public/lib/scheme_types
    yql/essentials/minikql
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/utils
)

SRCS(
    minikql.cpp
)

YQL_LAST_ABI_VERSION()

END()

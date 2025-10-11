LIBRARY()

SRCS(
    yql_simple_arrow_resolver.cpp
)

PEERDIR(
    yql/essentials/minikql/arrow
    yql/essentials/public/udf
    yql/essentials/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()

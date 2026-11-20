UNITTEST()

SRCS(
    qlfilter_ut.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/utils
    yt/yql/providers/yt/lib/qlfilter
)

YQL_LAST_ABI_VERSION()

END()

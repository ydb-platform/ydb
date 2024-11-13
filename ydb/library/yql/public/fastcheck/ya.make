LIBRARY()

SRCS(
    fastcheck.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
)

END()

RECURSE_FOR_TESTS(
    ut
)

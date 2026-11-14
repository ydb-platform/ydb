LIBRARY()

SRCS(
    pg_policy.cpp
)

PEERDIR(
    yql/essentials/core
    yql/essentials/core/services
    yql/essentials/core/type_ann
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)

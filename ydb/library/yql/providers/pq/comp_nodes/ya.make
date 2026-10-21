LIBRARY()

SRCS(
    dq_pq_parsing_wrapper.cpp
    yql_pq_factory.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/computation
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

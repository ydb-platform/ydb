LIBRARY()

SRCS(
    dq_fake_ca.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/core/testlib/basics
    yql/essentials/minikql/computation
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/providers/common/comp_nodes
)

YQL_LAST_ABI_VERSION()

END()

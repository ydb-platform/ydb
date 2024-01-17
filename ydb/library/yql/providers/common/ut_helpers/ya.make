LIBRARY()

SRCS(
    dq_fake_ca.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/core/testlib/basics
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/providers/common/comp_nodes
)

YQL_LAST_ABI_VERSION()

END()

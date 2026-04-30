LIBRARY()

SRCS(
    dq_fake_ca.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/library/actors/testlib
    ydb/library/mkql_proto/protos
    ydb/library/services
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/dq/proto
    yql/essentials/minikql/computation
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/providers/common/comp_nodes
)

YQL_LAST_ABI_VERSION()

END()

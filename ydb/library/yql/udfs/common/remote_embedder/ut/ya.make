UNITTEST()

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    remote_embedder_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/mock_server
    library/cpp/time_provider
    ydb/library/yql/udfs/common/remote_embedder
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()

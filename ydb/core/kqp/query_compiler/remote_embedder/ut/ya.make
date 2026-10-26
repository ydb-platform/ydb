UNITTEST()

SRCS(
    secrets_ut.cpp
)

PEERDIR(
    ydb/core/kqp/query_compiler/remote_embedder
    yql/essentials/providers/common/structured_token
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

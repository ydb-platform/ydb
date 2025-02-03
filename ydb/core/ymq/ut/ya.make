UNITTEST()

SRCS(
    queue_id_ut.cpp
    params_ut.cpp
)

PEERDIR(
    ydb/core/ymq/base
    ydb/core/ymq/http
    ydb/library/http_proxy/error
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()

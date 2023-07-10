UNITTEST()

SRCS(
    queue_id_ut.cpp
    params_ut.cpp
)

PEERDIR(
    ydb/core/ymq/base
    ydb/core/ymq/http
    ydb/library/http_proxy/error
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/exception_policy
)

END()

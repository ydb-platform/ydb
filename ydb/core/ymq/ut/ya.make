UNITTEST()

SRCS(
    queue_id_ut.cpp
    params_ut.cpp
)

PEERDIR(
    ydb/core/ymq/base
    ydb/core/ymq/http
    ydb/library/http_proxy/error
)

END()

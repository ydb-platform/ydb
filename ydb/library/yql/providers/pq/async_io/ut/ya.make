UNITTEST()

SRCS(
    dq_pq_cpu_quota_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/message_stream/async_io
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()

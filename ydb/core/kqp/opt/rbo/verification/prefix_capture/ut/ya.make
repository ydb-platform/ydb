UNITTEST()

SIZE(SMALL)

SRCS(
    capture_ut.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/kqp/opt/rbo/verification/prefix_capture
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

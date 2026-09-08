UNITTEST_FOR(ydb/core/tx/datashard)

PEERDIR(
    ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    export_s3_buffer_ut.cpp
)

END()

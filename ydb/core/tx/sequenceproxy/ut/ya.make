UNITTEST_FOR(ydb/core/tx/sequenceproxy)

SRCS(
    sequenceproxy_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()

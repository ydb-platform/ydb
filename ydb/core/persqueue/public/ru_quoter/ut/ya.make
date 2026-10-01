UNITTEST_FOR(ydb/core/persqueue/public/ru_quoter)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

SRCS(
    ru_quoter_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    ydb/core/metering
    ydb/core/quoter/public
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/library/aclib
)

END()

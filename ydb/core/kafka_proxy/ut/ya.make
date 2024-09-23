UNITTEST_FOR(ydb/core/kafka_proxy)

SIZE(medium)
TIMEOUT(600)

SRCS(
    ut_kafka_functions.cpp
    ut_protocol.cpp
    ut_serialization.cpp
    metarequest_ut.cpp
)

PEERDIR(
    ydb/core/kafka_proxy
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils

)
YQL_LAST_ABI_VERSION()
END()

UNITTEST_FOR(ydb/core/persqueue/pqtablet/partition/mirrorer)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    mirrorer_actor_ut.cpp
)

PEERDIR(
    library/cpp/string_utils/base64
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/library/kafka
)

END()

UNITTEST_FOR(ydb/core/persqueue/pqtablet/partition/mlp)

YQL_LAST_ABI_VERSION()

SRCS(
    mlp_storage_ut.cpp
    mlp_consumer_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/public/mlp/ut/common
)

END()

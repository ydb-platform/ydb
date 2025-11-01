UNITTEST_FOR(ydb/core/persqueue/pqtablet/partition/mlp)

YQL_LAST_ABI_VERSION()

SRCS(
    mlp_storage_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()

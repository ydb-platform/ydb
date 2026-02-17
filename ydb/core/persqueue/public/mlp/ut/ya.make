UNITTEST_FOR(ydb/core/persqueue/public/mlp)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)
#TIMEOUT(60)

SRCS(
    mlp_changer_ut.cpp
    mlp_purger_ut.cpp
    mlp_reader_ut.cpp
    mlp_writer_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/public/mlp/ut/common
)
ENV(INSIDE_YDB="1")
END()

RECURSE(
    common
)

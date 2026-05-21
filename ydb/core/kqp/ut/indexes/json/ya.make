UNITTEST_FOR(ydb/core/kqp)

REQUIREMENTS(cpu:2)

FORK_SUBTESTS()
SPLIT_FACTOR(100)
SIZE(MEDIUM)

SRCS(
    kqp_json_index_corpus.cpp
    kqp_json_index_predicate.cpp
    kqp_indexes_json_corpus_ut.cpp
    kqp_indexes_json_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()

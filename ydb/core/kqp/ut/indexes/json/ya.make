UNITTEST_FOR(ydb/core/kqp)

REQUIREMENTS(cpu:2)

FORK_SUBTESTS()
SPLIT_FACTOR(200)
SIZE(MEDIUM)

SRCS(
    kqp_indexes_json_auto_select_ut.cpp
    GLOBAL kqp_indexes_json_corpus_je_ut.cpp
    GLOBAL kqp_indexes_json_corpus_jejv_ut.cpp
    GLOBAL kqp_indexes_json_corpus_jv_ut.cpp
    kqp_indexes_json_tokens_ut.cpp
    kqp_indexes_json_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/indexes/json/common
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/adapters/issue
    ydb/library/json_index
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    common
)

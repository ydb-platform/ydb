LIBRARY()

SRCS(
    kqp_indexes_json_ut_common.cpp
    kqp_indexes_json_corpus.cpp
    kqp_indexes_json_predicate.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/core/kqp/ut/common
    ydb/library/json_index
)

YQL_LAST_ABI_VERSION()

END()

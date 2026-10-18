LIBRARY()

SRCS(
    analyzer.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/services/sql/parse
)

END()

RECURSE_FOR_TESTS(
    ut
)

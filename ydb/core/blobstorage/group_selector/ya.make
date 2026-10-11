LIBRARY()

SRCS(
    group_selector.cpp
)

PEERDIR(
    library/cpp/html/escape
    ydb/core/mon
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)

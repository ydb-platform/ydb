UNITTEST()

FORK_SUBTESTS()

SRCS(
    test_spans.cpp
    main.cpp
)

PEERDIR(
    ydb/library/actors/retro_tracing
    ydb/library/actors/testlib
)

END()

UNITTEST()

FORK_SUBTESTS()
REQUIREMENTS(cpu:1)
SRCS(
    main.cpp
)
PEERDIR(
    ydb/library/lockfree_bucket
)

END()

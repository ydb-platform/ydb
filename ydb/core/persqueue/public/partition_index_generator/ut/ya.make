UNITTEST_FOR(ydb/core/persqueue/public/partition_index_generator)

REQUIREMENTS(cpu:1)
SRCS(
    partition_index_generator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()

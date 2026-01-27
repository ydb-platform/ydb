UNITTEST_FOR(ydb/core/nbs/cloud/storage/core/libs/common)

SRCDIR(ydb/core/nbs/cloud/storage/core/libs/common)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/common

    library/cpp/testing/gmock_in_unittest
)

SRCS(
    block_buffer_ut.cpp
    block_data_ref_ut.cpp
    context_ut.cpp
    guarded_sglist_ut.cpp
    sglist_iter_ut.cpp
    sglist_test.cpp
    sglist_ut.cpp
)

END()

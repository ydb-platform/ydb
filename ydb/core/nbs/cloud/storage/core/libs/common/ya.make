LIBRARY()

GENERATE_ENUM_SERIALIZATION(error.h)

SRCS(
    affinity.cpp
    block_buffer.cpp
    block_data_ref.cpp
    context.cpp
    error.cpp
    guarded_sglist.cpp
    helpers.cpp
    page_size.cpp
    scheduler_test.cpp
    scheduler.cpp
    sglist_iter.cpp
    sglist_test.cpp
    sglist.cpp
    startable.cpp
    thread.cpp
    timer.cpp
    timer_test.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos

    ydb/library/actors/prof

    library/cpp/lwtrace
    library/cpp/json/writer
    library/cpp/threading/future

    util
)

END()

RECURSE_FOR_TESTS(
    ut
)

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
    sglist_iter.cpp
    sglist.cpp
    startable.cpp
    thread.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/lwtrace
    library/cpp/json/writer
    library/cpp/threading/future

    util
)

END()

RECURSE_FOR_TESTS(
    ut
)

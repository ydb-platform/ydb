LIBRARY()

SRCS(
    blob.cpp
    blob_serialization.cpp
    header.cpp
)



PEERDIR(
    ydb/core/base
    ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    yql_qstorage_file.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/qplayer/storage/memory
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)

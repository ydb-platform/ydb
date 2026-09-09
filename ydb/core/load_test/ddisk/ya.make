LIBRARY()

SRCS(
    ../ddisk_load.cpp
    ../persistent_buffer_write.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
    library/cpp/time_provider
    ydb/core/base
    ydb/core/blobstorage/ddisk
    ydb/core/control/lib
    ydb/core/load_test/common
)

END()

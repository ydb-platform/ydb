UNITTEST()

FORK_SUBTESTS(MODULO)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/protobuf/util
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/load_test/blobstorage
    ydb/core/load_test/ddisk
    ydb/core/load_test/nbs
)

YQL_LAST_ABI_VERSION()

SRCS(
    ddisk_load_ut.cpp
    group_test_ut.cpp
    nbs_dbg_like_alloc_helper_ut.cpp
    util_ut.cpp
    nbs_dbg_like_load_registry_ut.cpp
)

END()

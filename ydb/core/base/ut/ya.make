UNITTEST_FOR(ydb/core/base)

FORK_SUBTESTS()
TIMEOUT(600)
SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
)

SRCS(
    path_ut.cpp
    blobstorage_grouptype_ut.cpp
    localdb_ut.cpp
    logoblob_ut.cpp
    statestorage_ut.cpp
    statestorage_guardian_impl_ut.cpp
)

END()

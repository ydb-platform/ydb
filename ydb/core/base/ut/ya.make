UNITTEST_FOR(ydb/core/base)

OWNER(fomichev g:kikimr)

FORK_SUBTESTS()
TIMEOUT(600)
SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
)

SRCS(
    blobstorage_grouptype_ut.cpp
    localdb_ut.cpp
    logoblob_ut.cpp
    shared_data_ut.cpp
    statestorage_ut.cpp
    statestorage_guardian_impl_ut.cpp
)

END()

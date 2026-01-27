UNITTEST_FOR(ydb/core/base)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    blobstorage_grouptype_ut.cpp
    fulltext_ut.cpp
    kmeans_ut.cpp
    localdb_ut.cpp
    logoblob_ut.cpp
    memory_stats_ut.cpp
    path_ut.cpp
    statestorage_guardian_impl_ut.cpp
    statestorage_ut.cpp
    table_index_ut.cpp
)

END()

UNITTEST_FOR(ydb/core/blobstorage/vdisk/defrag)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/blobstorage/vdisk/defrag
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/huge
)

SRCS(
    defrag_actor_ut.cpp
    defrag_search_ut.cpp
)

END()

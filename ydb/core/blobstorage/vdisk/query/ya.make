LIBRARY()

PEERDIR(
    library/cpp/streams/bzip2
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/hulldb/barriers
    ydb/core/blobstorage/vdisk/hulldb/base
)

SRCS(
    defs.h
    assimilation.cpp
    assimilation.h
    query_barrier.cpp
    query_base.h
    query_dumpdb.h
    query_extr.cpp
    query_public.cpp
    query_public.h
    query_range.cpp
    query_readactor.cpp
    query_readactor.h
    query_readbatch.cpp
    query_readbatch.h
    query_spacetracker.h
    query_statalgo.h
    query_statdb.cpp
    query_statdb.h
    query_stathuge.cpp
    query_stathuge.h
    query_stattablet.cpp
    query_stattablet.h
    query_stream.h
)

END()

RECURSE_FOR_TESTS(
    ut
)

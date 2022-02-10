LIBRARY()

OWNER(
    fomichev
    g:kikimr
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
)

SRCS(
    defrag_actor.cpp
    defrag_actor.h
    defrag_quantum.cpp
    defrag_quantum.h
    defrag_rewriter.cpp
    defrag_rewriter.h
    defrag_search.h
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)

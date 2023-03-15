LIBRARY()

SRCS(
    defs.h
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/incrhuge
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/other
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/storagepoolmon
    ydb/core/blobstorage/vdisk
)

IF (MSVC)
    CFLAGS(
        /wd4503
    )
ENDIF()

END()

RECURSE(
    backpressure
    base
    crypto
    dsproxy
    groupinfo
    incrhuge
    lwtrace_probes
    nodewarden
    other
    pdisk
    storagepoolmon
    testing
    vdisk
)

RECURSE_FOR_TESTS(
    ut_blobstorage
    ut_group
    ut_mirror3of4
    ut_pdiskfit
    ut_testshard
    ut_vdisk
    ut_vdisk2
)

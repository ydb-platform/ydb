G_BENCHMARK(core_blobstorage_vdisk_hulldb_compstrat_benchmark)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
SIZE(LARGE)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/compstrat
)

END()

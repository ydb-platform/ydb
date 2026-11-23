PROGRAM(selector_coro_bench)

SRCS(main.cpp)

PEERDIR(
    ydb/apps/version
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/hulldb/test
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()

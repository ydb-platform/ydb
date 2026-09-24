UNITTEST_FOR(ydb/core/tx/datashard)

PEERDIR(
<<<<<<< HEAD
=======
    contrib/libs/apache/arrow
    library/cpp/streams/zstd
>>>>>>> 1f328a7e88d (fix growing uncompressed bytes (#52143))
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    export_s3_buffer_ut.cpp
)

END()

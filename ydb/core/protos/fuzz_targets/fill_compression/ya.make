FUZZ()

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/ydb_convert
    ydb/core/protos
    ydb/library/yverify_stream
    ydb/public/api/protos
)

END()

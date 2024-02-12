PROGRAM(s3_meta)

OWNER(senya)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp/client/ydb_table
    library/cpp/threading/local_executor
    util
)

SRCS(
    main.cpp
)

END()

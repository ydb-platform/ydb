FUZZ()

PEERDIR(
    ydb/core/protos
    ydb/core/ydb_convert
    ydb/core/scheme
    ydb/library/mkql_proto
    library/cpp/protobuf/json
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf/arrow
    yql/essentials/sql/pg_dummy
    contrib/libs/protobuf-mutator
)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

END()

FUZZ()

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/protos
    ydb/core/scheme
    ydb/core/ydb_convert
    ydb/library/mkql_proto
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

END()

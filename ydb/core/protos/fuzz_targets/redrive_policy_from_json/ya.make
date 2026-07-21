FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/ymq/base
    ydb/core/scheme
    ydb/library/mkql_proto
    library/cpp/protobuf/json
    library/cpp/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf/arrow
    yql/essentials/sql/pg_dummy
    contrib/libs/protobuf-mutator
)

CFLAGS(-Wno-deprecated-declarations)

END()

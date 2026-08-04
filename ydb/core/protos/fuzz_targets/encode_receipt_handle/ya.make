FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/ymq/base
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/core/ymq/proto
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

CFLAGS(-Wno-deprecated-declarations)

END()

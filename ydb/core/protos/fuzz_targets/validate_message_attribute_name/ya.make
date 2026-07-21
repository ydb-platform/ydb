FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/ymq/base
    library/cpp/json
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

CFLAGS(-Wno-deprecated-declarations)

END()

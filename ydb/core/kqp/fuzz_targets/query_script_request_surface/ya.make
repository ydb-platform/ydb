FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/fq/libs/common
    ydb/core/kqp/common
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()

FUZZ()

SIZE(MEDIUM)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()

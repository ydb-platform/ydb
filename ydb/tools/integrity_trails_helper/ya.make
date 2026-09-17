PROGRAM()

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/engine
    ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()

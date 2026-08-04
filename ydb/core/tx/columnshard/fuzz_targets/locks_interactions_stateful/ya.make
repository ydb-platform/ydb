FUZZ()

SIZE(MEDIUM)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/predicate
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

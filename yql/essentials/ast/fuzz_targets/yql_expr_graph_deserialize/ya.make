FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/ast/serialize
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf/arrow
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/libfuzzer
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/providers/common/schema/expr
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

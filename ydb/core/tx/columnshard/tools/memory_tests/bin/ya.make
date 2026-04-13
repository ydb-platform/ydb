PROGRAM(memory_tests)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    ydb/core/formats/arrow
)

SRCDIR(
    ydb/core/tx/columnshard/tools/memory_tests
)

SRCS(
    main.cpp
)

YQL_LAST_ABI_VERSION()

END()

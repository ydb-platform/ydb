PROGRAM()

NO_BUILD_IF(EXPORT_CMAKE)

SRCS(
    types_dump.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
    yql/essentials/parser/pg_catalog
    yql/essentials/utils/backtrace
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)

PROGRAM()

PEERDIR(
    library/cpp/getopt
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/accessor/common
    ydb/core/formats/arrow/serializer
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
)

YQL_LAST_ABI_VERSION()

NO_COMPILER_WARNINGS()

SRCS(
    main.cpp
)

END()

FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/common
    ydb/core/formats/arrow/accessor/sub_columns
    yql/essentials/types/binary_json
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

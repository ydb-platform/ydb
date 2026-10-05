UNITTEST_FOR(ydb/core/formats/arrow/accessor/sub_columns)

SIZE(SMALL)

PEERDIR(
    yql/essentials/types/binary_json
    ydb/core/formats/arrow/accessor/composite
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/accessor/sub_columns/ut_common
    ydb/library/arrow_kernels
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/core/formats/arrow
)

SRCS(
    ut_sub_columns.cpp
    ut_native_scalars.cpp
    ut_dictionary.cpp
    ut_sparsed.cpp
)

YQL_LAST_ABI_VERSION()

END()

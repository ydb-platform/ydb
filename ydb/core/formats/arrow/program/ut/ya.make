UNITTEST_FOR(ydb/core/formats/arrow/program)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/sparsed
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/filter
    ydb/core/formats/arrow/program
    ydb/core/formats/arrow/serializer
<<<<<<< HEAD
    ydb/library/arrow_kernels
=======
    ydb/library/formats/arrow/helpers
>>>>>>> 36847f09751 (Avoid sparse materialization for neutral AND/OR operands (#56229))
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_kernel_logic.cpp
    ut_collection.cpp
    ut_functions.cpp
)

YQL_LAST_ABI_VERSION()

END()

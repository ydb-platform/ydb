UNITTEST()

SRCS(
    array_builder_ut.cpp
    bit_util_ut.cpp
    block_reader_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
)

YQL_LAST_ABI_VERSION()

END()

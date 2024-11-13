UNITTEST()

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/no_llvm
    ydb/library/yql/public/purecalc/io_specs/mkql/no_llvm
    ydb/library/yql/public/purecalc/ut/lib
)

YQL_LAST_ABI_VERSION()

SRCDIR(
   ydb/library/yql/public/purecalc/io_specs/mkql/ut
)

SRCS(
    test_spec.cpp
)

END()

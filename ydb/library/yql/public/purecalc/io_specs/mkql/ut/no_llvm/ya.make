UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(300)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
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

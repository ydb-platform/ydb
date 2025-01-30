UNITTEST()

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/public/purecalc
    yql/essentials/public/purecalc/io_specs/arrow
    yql/essentials/public/purecalc/ut/lib
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()

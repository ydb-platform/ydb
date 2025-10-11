LIBRARY()

SRCS(
    purecalc.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/public/purecalc/common
)

YQL_LAST_ABI_VERSION()

PROVIDES(YQL_PURECALC)

END()

RECURSE(
    common
    examples
    helpers
    io_specs
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)

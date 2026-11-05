LIBRARY()

SRCS(
    yql/essentials/public/fastcheck/fastcheck.cpp
    yql/essentials/public/fastcheck/linter.cpp
    yql/essentials/public/fastcheck/parse_checks.cpp
)

PEERDIR(
    yql/essentials/public/fastcheck/abi
    yql/essentials/public/fastcheck/abi/interface
    yql/essentials/public/fastcheck/abi/serialization
    yql/essentials/public/udf_meta/interface
)

GENERATE_ENUM_SERIALIZATION(yql/essentials/public/fastcheck/linter.h)

END()

RECURSE(
    abi
)

RECURSE_FOR_TESTS(
    ut
)

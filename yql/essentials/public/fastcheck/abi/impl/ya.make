LIBRARY()

SRCS(
    fastcheck.cpp
    linter.cpp
    result.cpp
)

PEERDIR(
    yql/essentials/public/abi_impl
    yql/essentials/public/fastcheck/abi/interface
    yql/essentials/public/fastcheck/abi/serialization
    yql/essentials/public/fastcheck/internal
    yql/essentials/public/udf_meta
)

END()

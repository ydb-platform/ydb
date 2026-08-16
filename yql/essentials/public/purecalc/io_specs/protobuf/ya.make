LIBRARY()

PEERDIR(
    yql/essentials/public/purecalc/common
    yql/essentials/public/purecalc/io_specs/protobuf_raw
)

SRCS(
    spec.cpp
    proto_variant.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

ADDINCL(
    yql/essentials/public/purecalc/io_specs/protobuf
)

SRCDIR(
    yql/essentials/public/purecalc/io_specs/protobuf
)

SRCS(
    spec.cpp
    proto_variant.cpp
)

PEERDIR(
    yql/essentials/public/purecalc/common/no_llvm
    yql/essentials/public/purecalc/io_specs/protobuf_raw/no_llvm
)

YQL_LAST_ABI_VERSION()

END()

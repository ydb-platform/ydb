LIBRARY()

ADDINCL(
    yql/essentials/public/purecalc/io_specs/protobuf_raw
)

SRCDIR(
    yql/essentials/public/purecalc/io_specs/protobuf_raw
)

SRCS(
    proto_holder.cpp
    spec.cpp
)

PEERDIR(
    yql/essentials/public/purecalc/common/no_llvm
    yql/essentials/public/purecalc/helpers/protobuf
)

YQL_LAST_ABI_VERSION()

END()

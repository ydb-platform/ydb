LIBRARY()

PEERDIR(
    yql/essentials/public/purecalc/common
    yql/essentials/public/purecalc/helpers/protobuf
)

SRCS(
    proto_holder.cpp
    spec.cpp
    spec.h
)

YQL_LAST_ABI_VERSION()

END()

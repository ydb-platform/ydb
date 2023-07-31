LIBRARY()

PEERDIR(
    ydb/library/yql/public/purecalc/common
    ydb/library/yql/public/purecalc/helpers/protobuf
)

SRCS(
    proto_holder.cpp
    spec.cpp
    spec.h
)

YQL_LAST_ABI_VERSION()

END()

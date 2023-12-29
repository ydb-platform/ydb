LIBRARY()

SRCS(
    change_record.cpp
)

GENERATE_ENUM_SERIALIZATION(change_record.h)

PEERDIR(
    ydb/core/protos
    ydb/core/scheme
)

YQL_LAST_ABI_VERSION()

END()

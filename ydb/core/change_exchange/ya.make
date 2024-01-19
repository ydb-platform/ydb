LIBRARY()

SRCS(
    change_exchange.cpp
    change_record.cpp
)

GENERATE_ENUM_SERIALIZATION(change_record.h)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
)

YQL_LAST_ABI_VERSION()

END()

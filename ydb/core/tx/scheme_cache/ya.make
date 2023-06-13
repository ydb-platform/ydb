LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/scheme
    ydb/library/aclib
)

SRCS(
    scheme_cache.cpp
)

GENERATE_ENUM_SERIALIZATION(scheme_cache.h)

YQL_LAST_ABI_VERSION()

END()

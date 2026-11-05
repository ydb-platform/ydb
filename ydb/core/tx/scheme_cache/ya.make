YQL_LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/scheme
    ydb/core/persqueue/writer
    ydb/library/aclib
)

SRCS(
    scheme_cache.cpp
)

GENERATE_ENUM_SERIALIZATION(scheme_cache.h)

END()

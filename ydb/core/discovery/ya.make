LIBRARY()

SRCS(
    discovery.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    library/cpp/actors/core
)

END()

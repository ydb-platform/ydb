LIBRARY()

SRCS(
    events.h
    resolver_validation.h
    source.h
    source.cpp
    support_links_resolver.h
    support_links_resolver.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/meta/protos
)

END()

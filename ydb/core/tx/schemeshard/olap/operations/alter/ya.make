LIBRARY()

SRCS(
    abstract.cpp
    standalone.cpp
    in_store.cpp
    converter.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()

LIBRARY()

SRCS(
    console.cpp
    console.h
    configs_dispatcher.h
    configs_dispatcher_observer.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/yverify_stream
    ydb/public/api/protos
)

END()

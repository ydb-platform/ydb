LIBRARY()

SRCS(
    simple_core_facility.cpp
    simple_core_facility.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/sdk/cpp/src/library/time
)

END()

LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/core/fq/libs/checkpointing_common
)

END()

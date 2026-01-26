LIBRARY()

SRCS(
    direct_block_group.cpp
    request.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/mind/bscontroller
)

END()

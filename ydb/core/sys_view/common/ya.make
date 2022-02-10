LIBRARY()

OWNER(
    monster
    g:kikimr
)

SRCS(
    common.h
    events.h
    keys.h
    path.h
    scan_actor_base_impl.h
    schema.h
    schema.cpp
    utils.h
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base 
    ydb/core/protos 
    ydb/core/tablet_flat 
)

END()

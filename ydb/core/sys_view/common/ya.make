LIBRARY()

SRCS(
    common.h
    events.h
    keys.h
    path.h
    scan_actor_base_impl.h
    schema.h
    schema.cpp
    utils.h
    processor_scan.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet_flat
    library/cpp/deprecated/atomic
    ydb/library/yql/parser/pg_wrapper/interface
)

END()

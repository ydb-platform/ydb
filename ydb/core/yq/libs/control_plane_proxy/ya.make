OWNER(g:yq)

LIBRARY()

SRCS(
    control_plane_proxy.cpp
    probes.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/mon
    ydb/core/yq/libs/actors/logging 
    ydb/core/yq/libs/actors
    ydb/core/yq/libs/control_plane_proxy/events
    ydb/core/yq/libs/control_plane_storage
    ydb/library/folder_service
    ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)

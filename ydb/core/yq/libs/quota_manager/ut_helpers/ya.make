LIBRARY()

SRCS(
    fake_quota_manager.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/yq/libs/quota_manager/events
)

END()

LIBRARY()

SRCS(
    fake_quota_manager.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/quota_manager/events
)

END()

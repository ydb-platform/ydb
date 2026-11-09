UNITTEST_FOR(ydb/apps/bridge_skipper/domain)

SRCS(
    admin_ut.cpp
    health_ut.cpp
    policy_ut.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/testing/common
)

DATA(
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/blue_only.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/blue_with_orange.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/bsc_clean_with_pile_issues.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/bsc_red.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/fallback_grey.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/fallback_red.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/stage2.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/truncated.pbtxt
    arcadia/ydb/apps/bridge_skipper/domain/ut/fixtures/unattributed.pbtxt
)

END()

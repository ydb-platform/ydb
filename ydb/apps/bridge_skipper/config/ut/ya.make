UNITTEST()

SRCS(
    config_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    ydb/apps/bridge_skipper/config
)

DATA(
    arcadia/ydb/apps/bridge_skipper/config/example.yaml
)

END()

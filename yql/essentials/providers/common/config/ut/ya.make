UNITTEST_FOR(yql/essentials/providers/common/config)

SRCS(
    yql_config_ut.cpp
    yql_config_qplayer_ut.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/memory
)

YQL_LAST_ABI_VERSION()

END()

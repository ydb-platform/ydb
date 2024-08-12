UNITTEST_FOR(ydb/core/cms/console)

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/protobuf/util
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    configs_cache_ut.cpp
    configs_dispatcher_ut.cpp
    console_ut_tenants.cpp
    console_ut_configs.cpp
    immediate_controls_configurator_ut.cpp
    log_settings_configurator_ut.cpp
    modifications_validator_ut.cpp
    net_classifier_updater_ut.cpp
    jaeger_tracing_configurator_ut.cpp
)

END()

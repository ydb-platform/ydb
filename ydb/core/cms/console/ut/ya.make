UNITTEST_FOR(ydb/core/cms/console)

OWNER(g:kikimr)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    SPLIT_FACTOR(20)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(ram:16)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/protobuf/util
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
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
)

END()

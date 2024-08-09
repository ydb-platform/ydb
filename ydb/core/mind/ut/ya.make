UNITTEST_FOR(ydb/core/mind)

FORK_SUBTESTS()

IF (SANITIZER_TYPE  == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    SPLIT_FACTOR(80)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SPLIT_FACTOR(80)
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(
        ram:16
    )
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
)

SRCS(
    node_broker_ut.cpp
    tenant_ut_local.cpp
    tenant_ut_pool.cpp
    tenant_node_enumeration_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()

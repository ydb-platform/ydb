UNITTEST_FOR(ydb/core/client/minikql_result_lib)

FORK_SUBTESTS()

TIMEOUT(300)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    converter_ut.cpp
    objects_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(network:full ram:13)

END()

UNITTEST_FOR(ydb/core/client/minikql_result_lib)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    converter_ut.cpp
    objects_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(network:full)

END()

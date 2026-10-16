UNITTEST_FOR(ydb/library/workload/tpch)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRCS(queries_ut.cpp)

PEERDIR(
    library/cpp/regex/pcre
)

END()

UNITTEST_FOR(ydb/library/workload/tpch)

SIZE(SMALL)

SRCS(queries_ut.cpp)

PEERDIR(
    library/cpp/regex/pcre
)

END()

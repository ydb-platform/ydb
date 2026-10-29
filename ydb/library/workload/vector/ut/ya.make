UNITTEST_FOR(ydb/library/workload/vector)

SRCS(
    float8_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/lib/ydb_cli/import
)

END()

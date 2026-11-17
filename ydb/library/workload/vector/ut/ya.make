UNITTEST_FOR(ydb/library/workload/vector)

SRCS(
    file_import_ut.cpp
    float8_ut.cpp
    int4_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/lib/ydb_cli/import
)

END()

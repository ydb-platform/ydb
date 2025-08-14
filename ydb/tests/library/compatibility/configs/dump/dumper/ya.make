PROGRAM(ydb-config-meta-dumper)

PEERDIR(
    ydb/core/protos
    library/cpp/json
    library/cpp/getopt
    library/cpp/svnversion
)

SRCS(main.cpp)

END()

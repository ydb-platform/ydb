PROGRAM(ic_bench)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/mock
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/ut/lib/port_manager
    library/cpp/getopt/small
    library/cpp/monlib/dynamic_counters
)

END()

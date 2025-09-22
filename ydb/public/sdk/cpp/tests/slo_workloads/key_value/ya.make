SUBSCRIBER(
    pnv1
    g:kikimr
)

PROGRAM()

SRCS(
    create.cpp
    drop.cpp
    generate.cpp
    main.cpp
    run.cpp
    key_value.h
    key_value.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/tests/slo_workloads/utils
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/table
)

END()

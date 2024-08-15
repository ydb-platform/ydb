PROGRAM()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    main.cpp
    vector_index.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/table
)

END()

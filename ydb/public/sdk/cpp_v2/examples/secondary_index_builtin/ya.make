PROGRAM()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    main.cpp
    secondary_index.cpp
    secondary_index_create.cpp
    secondary_index_fill.cpp
    secondary_index_select.cpp
    secondary_index_drop.cpp
    secondary_index_select_join.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp_v2/src/client/table
)

END()

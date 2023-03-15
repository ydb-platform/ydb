PROGRAM()

SRCS(
    main.cpp
    secondary_index.cpp
    secondary_index_create.cpp
    secondary_index_delete.cpp
    secondary_index_drop.cpp
    secondary_index_generate.cpp
    secondary_index_list.cpp
    secondary_index_update.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/client/ydb_table
)

END()

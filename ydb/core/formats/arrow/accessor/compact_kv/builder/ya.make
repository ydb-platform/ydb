PROGRAM()

PEERDIR(
    contrib/libs/apache/avro
    library/cpp/getopt
    library/cpp/json
    library/cpp/streams/zstd
)

ADDINCL(
    contrib/libs/apache/avro/include
)

NO_COMPILER_WARNINGS()

SRCS(
    main.cpp
)

END()

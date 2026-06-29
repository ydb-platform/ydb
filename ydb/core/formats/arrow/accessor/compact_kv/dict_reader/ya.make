PROGRAM()

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/streams/zstd
)

NO_COMPILER_WARNINGS()

SRCS(
    main.cpp
)

END()

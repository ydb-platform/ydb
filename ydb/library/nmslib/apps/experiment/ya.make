PROGRAM(experiment)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
)

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    main.cc
)

END()

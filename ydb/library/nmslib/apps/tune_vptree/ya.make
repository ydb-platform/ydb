PROGRAM(tune_vptree)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
)

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    tune_vptree.cc
)

END()

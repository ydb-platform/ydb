PROGRAM(bench_distfunc)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
)

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    bench_distfunc.cc
)

END()

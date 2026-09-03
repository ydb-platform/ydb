PROGRAM(dummy_app)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
)

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    dummy_app.cc
)

END()

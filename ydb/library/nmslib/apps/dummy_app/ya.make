PROGRAM(dummy_app)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
)

SRCS(
    dummy_app.cc
)

END()

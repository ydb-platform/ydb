PROGRAM(add_columns)

SRCS(main.cpp)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

CFLAGS(-mavx2)

PEERDIR(ydb/library/yql/utils/simd)

END()

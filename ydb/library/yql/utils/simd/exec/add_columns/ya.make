PROGRAM(add_columns)

SRCS(main.cpp)

SIZE(MEDIUM)

CFLAGS(-mavx2)

PEERDIR(ydb/library/yql/utils/simd)

END()

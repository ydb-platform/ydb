PROGRAM(merge_columns_benchmark)

SRCS(main.cpp)

CFLAGS(-mavx2)

PEERDIR(ydb/library/yql/utils/simd)

END()
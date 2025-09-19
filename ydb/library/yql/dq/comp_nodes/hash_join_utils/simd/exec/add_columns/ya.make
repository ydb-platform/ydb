PROGRAM(add_columns)

SRCS(main.cpp)

SIZE(MEDIUM)

CFLAGS(-mavx2)

PEERDIR(ydb/library/yql/dq/comp_nodes/hash_join_utils/simd)

END()

OWNER(g:yql)

PROGRAM()

SRCS(main.cpp)

PEERDIR(ydb/library/yql/utils/simd/exec/runtime_dispatching)

CFLAGS(-mavx2)

END()
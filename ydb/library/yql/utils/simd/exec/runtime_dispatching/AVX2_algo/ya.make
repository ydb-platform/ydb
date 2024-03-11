LIBRARY()

OWNER(g:yql)

CFLAGS(-mavx2)

SRCS(avx2_algo.cpp)

END()
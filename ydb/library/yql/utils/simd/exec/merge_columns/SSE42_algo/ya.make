LIBRARY()

OWNER(g:yql)

CFLAGS(-msse4.2)

SRCS(sse42_algo.cpp)

END()
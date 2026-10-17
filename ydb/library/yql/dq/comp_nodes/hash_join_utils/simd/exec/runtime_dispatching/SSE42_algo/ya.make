IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

# gn: move into parent

CFLAGS(-msse4.2)

SRCS(sse42_algo.cpp)

END()

ENDIF()
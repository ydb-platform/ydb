IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

# gn: move into parent

CFLAGS(-mavx2)

SRCS(avx2_algo.cpp)

END()

ENDIF()
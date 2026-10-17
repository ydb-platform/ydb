IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

# gn: move into parent

SRCS(fallback_algo.cpp)

END()

ENDIF()
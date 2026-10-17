IF (OS_LINUX)

PROGRAM(pire_compile_benchmark)

ALLOCATOR(SYSTEM)

SRCS(main.cpp)

PEERDIR(library/cpp/regex/pire)

END()

ENDIF()

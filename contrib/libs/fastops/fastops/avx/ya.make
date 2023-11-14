LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

SRCS(
    ops_avx.h
    ops_avx.cpp
)

PEERDIR(
    contrib/libs/fastops/fastops/core
)

IF (MSVC AND NOT CLANG_CL)
    CFLAGS(
        /arch:AVX
        /D__SSE4_1__=1
        /D__SSE4_2__=1
        -DNO_AVX2
    )
ELSEIF (CLANG_CL)
    CFLAGS(
        -mavx
        -msse4.1
        -msse4.2
        -DNO_AVX2
    )
ELSE()
    CFLAGS(
        -mavx
        -msse4
        -msse4.1
        -msse4.2
        -Wno-unknown-pragmas
        -Wno-unused-local-typedef
        -DNO_AVX2
    )
ENDIF()

END()

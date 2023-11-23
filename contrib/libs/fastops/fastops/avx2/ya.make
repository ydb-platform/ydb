LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

SRCS(
    ops_avx2.h
    ops_avx2.cpp
)

PEERDIR(
    contrib/libs/fastops/fastops/core
)

IF (MSVC AND NOT CLANG_CL)
    CFLAGS(
        /arch:AVX2
        /D__SSE4_1__=1
        /D__SSE4_2__=1
    )
ELSEIF (CLANG_CL)
    CFLAGS(
        -mavx
        -mavx2
        -mfma
        -msse4.1
        -msse4.2
    )
ELSE()
    CFLAGS(
        -mavx
        -mavx2
        -mfma
        -msse4
        -msse4.1
        -msse4.2
        -Wno-unknown-pragmas
        -Wno-unused-local-typedef
    )
ENDIF()

END()

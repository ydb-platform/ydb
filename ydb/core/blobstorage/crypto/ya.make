LIBRARY()

IF (NOT OS_WINDOWS AND NOT ARCH_ARM64)
    SRCS(
        chacha.cpp
        crypto.cpp
        crypto.h
        poly1305.cpp
        chacha_vec.cpp
        chacha_512.cpp
        poly1305_vec.cpp
        secured_block.cpp
    )
ELSE()
    SRCS(
        chacha.cpp
        crypto.cpp
        crypto.h
        poly1305.cpp
        secured_block.cpp
    )
ENDIF()

PEERDIR(
    contrib/libs/t1ha
    library/cpp/sse
    ydb/library/actors/util
)

END()

RECURSE_FOR_TESTS(
    ut
)

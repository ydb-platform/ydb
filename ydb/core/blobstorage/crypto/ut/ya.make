UNITTEST_FOR(ydb/core/blobstorage/crypto)

IF (NOT OS_WINDOWS AND NOT ARCH_ARM64)
    SRCS(
        chacha_ut.cpp
        chacha_vec_ut.cpp
        chacha_512_ut.cpp
        crypto_rope_ut.cpp
        crypto_ut.cpp
        poly1305_ut.cpp
        poly1305_vec_ut.cpp
    )
ELSE()
    SRCS(
        chacha_ut.cpp
        crypto_ut.cpp
        poly1305_ut.cpp
    )
ENDIF()

END()

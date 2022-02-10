UNITTEST_FOR(ydb/core/blobstorage/crypto)

OWNER(g:kikimr)

IF (NOT OS_WINDOWS) 
    SRCS(
        chacha_ut.cpp
        chacha_vec_ut.cpp
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

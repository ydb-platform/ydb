LIBRARY()

NO_UTIL()

IF (ARCH_X86_64 OR ARCH_I386)
    PEERDIR(
        library/cpp/digest/argonish/internal/proxies/macro
        library/cpp/digest/argonish/internal/argon2
        library/cpp/digest/argonish/internal/blake2b
    )
    SRC_C_SSE41(
        proxy_sse41.cpp
    )
ENDIF()

END()

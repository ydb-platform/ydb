LIBRARY()

NO_UTIL()

PEERDIR(
    library/cpp/digest/argonish/internal/proxies/macro
    library/cpp/digest/argonish/internal/argon2
    library/cpp/digest/argonish/internal/blake2b
)

SRCS(
    proxy_ref.cpp
)

END()

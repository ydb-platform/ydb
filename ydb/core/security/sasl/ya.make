LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/library/actors/core
    ydb/library/login/hashes_checker
    ydb/library/login/password_checker
    ydb/library/login/sasl
)

SRCS(
    hasher.cpp
)

END()

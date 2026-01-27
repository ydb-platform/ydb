LIBRARY()

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/protobuf
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/library/login/account_lockout
    ydb/library/login/cache
    ydb/library/login/hashes_checker
    ydb/library/login/protos
    ydb/library/login/password_checker
    ydb/library/login/sasl
)

SRCS(
    login.cpp
    login.h
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    account_lockout
    cache
    hashes_checker
    password_checker
    sasl
)

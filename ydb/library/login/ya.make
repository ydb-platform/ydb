LIBRARY()

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/protobuf
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/library/login/protos
    ydb/library/login/password_checker
    ydb/library/login/account_lockout
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
    password_checker
    account_lockout
)

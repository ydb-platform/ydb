FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/login/sasl
    library/cpp/string_utils/base64
    contrib/libs/openssl
)

END()

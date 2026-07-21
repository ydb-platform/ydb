FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/login
    ydb/library/login/sasl
)

YQL_LAST_ABI_VERSION()

END()

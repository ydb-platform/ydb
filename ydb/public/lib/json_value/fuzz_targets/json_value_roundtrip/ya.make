FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/json
    ydb/public/lib/json_value
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
)

END()

FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
)

END()

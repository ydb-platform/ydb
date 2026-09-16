LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/consumer
    yql/essentials/utils/json
    yql/essentials/utils/log
)

SRCS(
    consumer.cpp
    exception.cpp
    listener.cpp
    marshal.cpp
    message.cpp
)

END()

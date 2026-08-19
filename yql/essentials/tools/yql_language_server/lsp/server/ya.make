LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/consumer
    yql/essentials/tools/yql_language_server/lsp/json_rpc
    yql/essentials/tools/yql_language_server/lsp/message
)

SRCS(
    base_protocol.cpp
    parallel.cpp
    server.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/json_rpc
    yql/essentials/tools/yql_language_server/lsp/message
)

SRCS(
    position.cpp
    synchronization.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

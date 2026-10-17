LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/core
    yql/essentials/tools/yql_language_server/service
    yql/essentials/tools/yql_language_server/lsp/api
)

SRCS(
    api.cpp
)

END()

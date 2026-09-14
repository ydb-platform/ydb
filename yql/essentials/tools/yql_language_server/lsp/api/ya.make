LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/message
)

SRCS(
    api.cpp
    base.cpp
    completion.cpp
    diagnostic.cpp
    formatting.cpp
    session.cpp
    synchronization.cpp
)

END()

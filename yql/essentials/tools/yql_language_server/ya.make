PROGRAM()

PEERDIR(
    yql/essentials/tools/yql_language_server/api
    yql/essentials/tools/yql_language_server/service
    yql/essentials/tools/yql_language_server/lsp/server
    library/cpp/getopt
)

SRCS(
    args.cpp
    main.cpp
)

END()

RECURSE(
    api
    lsp
    service
    testing
)

PROGRAM()

PEERDIR(
    yql/essentials/tools/yql_language_server/api
    yql/essentials/tools/yql_language_server/service
    yql/essentials/tools/yql_language_server/lsp/server
    library/cpp/getopt
    library/cpp/time_provider
)

SRCS(
    args.cpp
    main.cpp
    message_capture.cpp
)

END()

RECURSE(
    api
    lsp
    service
    testing
)

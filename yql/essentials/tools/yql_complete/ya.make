IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    library/cpp/iterator
    yql/essentials/sql/v1/ide/completion
    yql/essentials/sql/v1/ide/completion/name/cluster/static
    yql/essentials/sql/v1/ide/completion/name/object/simple/static
    yql/essentials/sql/v1/ide/completion/name/service/ranking
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/completion/name/service/cluster
    yql/essentials/sql/v1/ide/completion/name/service/schema
    yql/essentials/sql/v1/ide/completion/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/utils
)

SRCS(
    yql_complete.cpp
)

END()

ENDIF()

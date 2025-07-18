IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    library/cpp/iterator
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/complete/name/cluster/static
    yql/essentials/sql/v1/complete/name/object/simple/static
    yql/essentials/sql/v1/complete/name/service/ranking
    yql/essentials/sql/v1/complete/name/service/static
    yql/essentials/sql/v1/complete/name/service/cluster
    yql/essentials/sql/v1/complete/name/service/schema
    yql/essentials/sql/v1/complete/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/utils
)

SRCS(
    yql_complete.cpp
)

END()

ENDIF()

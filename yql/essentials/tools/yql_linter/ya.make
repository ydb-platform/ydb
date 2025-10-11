IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    library/cpp/getopt
    yql/essentials/public/fastcheck
    library/cpp/colorizer
)

SRCS(
    yql_linter.cpp
)

END()

ENDIF()

IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

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

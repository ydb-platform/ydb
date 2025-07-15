IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    yql/essentials/sql/v1/highlight
)

SRCS(
    yql_highlight.cpp
)

END()

RECURSE(
    artifact
)

ENDIF()

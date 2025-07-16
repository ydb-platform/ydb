IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    yql/essentials/sql/v1/highlight
    yql/essentials/utils
)

SRCS(
    generate_textmate.cpp
    generate_vim.cpp
    generate.cpp
    json.cpp
    yql_highlight.cpp
)

END()

RECURSE(
    artifact
)

ENDIF()

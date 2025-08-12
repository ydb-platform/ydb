IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")
    PROGRAM()

    PEERDIR(
        library/cpp/getopt
        library/cpp/json
        library/cpp/on_disk/tar_archive
        yql/essentials/sql/v1/highlight
        yql/essentials/utils
    )

    SRCS(
        generator_highlight_js.cpp
        generator_json.cpp
        generator_monarch.cpp
        generator_textmate.cpp
        generator_vim.cpp
        generator.cpp
        highlighting.cpp
        json.cpp
        yql_highlight.cpp
        yqls_highlight.cpp
    )

    END()

    RECURSE(
        artifact
    )

    RECURSE_FOR_TESTS(
        ut
    )

ENDIF()

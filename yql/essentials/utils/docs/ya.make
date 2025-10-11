LIBRARY()

SRCS(
    link_page.cpp
    link.cpp
    markdown.cpp
    name.cpp
    page.cpp
    resource.cpp
    verification.cpp
)

PEERDIR(
    yql/essentials/utils
    yql/essentials/core/sql_types
    contrib/libs/re2
    library/cpp/resource
    library/cpp/json
    library/cpp/case_insensitive_string
)

END()

RECURSE_FOR_TESTS(
    ut
)

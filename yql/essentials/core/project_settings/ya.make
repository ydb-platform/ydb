LIBRARY()

SRCS(
    yql_project_settings.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/public/langver
)

END()

RECURSE_FOR_TESTS(
    ut
)

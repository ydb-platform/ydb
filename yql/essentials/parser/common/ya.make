LIBRARY()

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/core/issue
)

SRCS(
    error.cpp
    issue.cpp
)

END()

RECURSE(
    antlr4
)

LIBRARY()

SRCS(
    secrets.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/providers/common/structured_token
    yql/essentials/public/issue/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    operation_permission.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/providers/common/proto
)

END()

RECURSE_FOR_TESTS(
    ut
)

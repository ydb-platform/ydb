LIBRARY()

SRCS(
    case_helper.cpp
    index_defaults.cpp
    index_parameters.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    ydb/core/protos
)

SRCS(
    runtime_feature_flags.h
    runtime_feature_flags.cpp
)

END()

IF(NOT EXPORT_CMAKE)
    RECURSE(
        codegen
    )
ENDIF()

RECURSE_FOR_TESTS(
    ut
)

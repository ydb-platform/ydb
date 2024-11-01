LIBRARY()

PEERDIR(
    ydb/core/protos
)

IF (EXPORT_CMAKE)
    # No Python codegen in cmake, pregenerate from ya to compile
    SRCS(
        runtime_feature_flags.h
        runtime_feature_flags.cpp
    )
ELSE()
    RUN_PROGRAM(
        ydb/core/base/generated/codegen
            runtime_feature_flags.h.in
            runtime_feature_flags.h
        IN runtime_feature_flags.h.in
        OUT runtime_feature_flags.h
        OUTPUT_INCLUDES
            util/system/types.h
            atomic
            tuple
    )

    RUN_PROGRAM(
        ydb/core/base/generated/codegen
            runtime_feature_flags.cpp.in
            runtime_feature_flags.cpp
        IN runtime_feature_flags.cpp.in
        OUT runtime_feature_flags.cpp
        OUTPUT_INCLUDES
            ydb/core/base/generated/runtime_feature_flags.h
            ydb/core/protos/feature_flags.pb.h
    )
ENDIF()

END()

IF (NOT EXPORT_CMAKE)
    RECURSE(
        codegen
    )
ENDIF()

RECURSE_FOR_TESTS(
    ut
)

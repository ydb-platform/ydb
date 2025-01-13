LIBRARY()

PEERDIR(
    ydb/core/protos
)

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

END()

RECURSE(
    codegen
)

RECURSE_FOR_TESTS(
    ut
)

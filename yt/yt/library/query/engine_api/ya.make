LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    append_function_implementation.cpp
    column_evaluator.cpp
    config.cpp
    coordinator.cpp
    evaluation_helpers.cpp
    evaluator.cpp
    builtin_function_profiler.cpp
    range_inferrer.cpp
    new_range_inferrer.cpp
    position_independent_value.cpp
    position_independent_value_transfer.cpp
)

ADDINCL(
    contrib/libs/sparsehash/src
)

PEERDIR(
    yt/yt/core
    yt/yt/library/query/misc
    yt/yt/library/query/proto
    yt/yt/library/query/base
    yt/yt/client
    library/cpp/yt/memory
    contrib/libs/sparsehash
)

END()

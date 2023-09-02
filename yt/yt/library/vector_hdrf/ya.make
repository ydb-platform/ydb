LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    job_resources.cpp
    piecewise_linear_function_helpers.cpp
    resource_vector.cpp
    resource_volume.cpp
    # Files below this line, depends on core/
    resource_helpers.cpp
    fair_share_update.cpp
)

PEERDIR(
    yt/yt/library/numeric
    # Core dependencies.
    yt/yt/library/numeric/serialize
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)

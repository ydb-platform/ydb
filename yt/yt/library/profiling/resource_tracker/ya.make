LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    GLOBAL configure_resource_tracker.cpp
    resource_tracker.cpp
)

PEERDIR(
    yt/yt/library/profiling

    # TODO(prime@:) remove this, once dependency cycle with yt/core is resolved
    yt/yt_proto/yt/core
)

END()

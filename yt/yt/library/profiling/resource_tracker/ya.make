LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    resource_tracker.cpp
)

PEERDIR(
    yt/yt/library/profiling

    # TODO(prime@:) remove this, once dependency cycle with yt/core is resolved
    yt/yt_proto/yt/core
)

END()

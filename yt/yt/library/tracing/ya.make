LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    public.cpp
    tracer.cpp
    async_queue_trace.cpp
    batch_trace.cpp
)

PEERDIR(
    # TODO(prime@:) remove this, once ADDINCL(yt) is removed.
    yt/yt/build

    # TODO(prime@:) remove this, once dependency cycle with yt/core is resolved
    yt/yt_proto/yt/core
)

END()

RECURSE(
    jaeger
    example
    unittests
)

IF (NOT OPENSOURCE)
    # NB: default-linux-x86_64-relwithdebinfo-opensource build does not support python programs and modules.
    RECURSE(
        integration
        py
    )
ENDIF()

LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    objects_holder.cpp
    function_context.cpp
)

PEERDIR(
    library/cpp/yt/assert
)

END()

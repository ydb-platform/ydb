LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    objects_holder.cpp
    function_context.cpp
    rowset_subrange_reader.cpp
    rowset_writer.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/numeric
    library/cpp/yt/assert
    library/cpp/yt/memory
)

END()

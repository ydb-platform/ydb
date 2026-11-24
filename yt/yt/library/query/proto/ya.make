LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    query.proto
    query_service.proto
    functions_cache.proto
)

PEERDIR(
    yt/yt/client
)

END()

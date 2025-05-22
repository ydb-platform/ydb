PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

SRCS(
    crypto/proto/crypto.proto

    misc/proto/bloom_filter.proto
    misc/proto/error.proto
    misc/proto/guid.proto
    misc/proto/hyperloglog.proto
    misc/proto/protobuf_helpers.proto

    tracing/proto/span.proto
    tracing/proto/tracing_ext.proto

    bus/proto/bus.proto

    rpc/proto/rpc.proto

    yson/proto/protobuf_interop.proto

    ytree/proto/attributes.proto
    ytree/proto/request_complexity_limits.proto
    ytree/proto/ypath.proto
)

PROTO_NAMESPACE(yt)

PY_NAMESPACE(yt_proto.yt.core)

EXCLUDE_TAGS(GO_PROTO)

END()

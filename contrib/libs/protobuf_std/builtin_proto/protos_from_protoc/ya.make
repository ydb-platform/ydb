PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

EXCLUDE_TAGS(
    CPP_PROTO
    GO_PROTO
    JAVA_PROTO
    PY_PROTO
)

SET(USE_VANILLA_PROTOC yes)

NO_MYPY()

NO_OPTIMIZE_PY_PROTOS()

DISABLE(NEED_GOOGLE_PROTO_PEERDIRS)

PY_NAMESPACE(.)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/protoc_std/src
)

SRCDIR(contrib/libs/protoc_std/src)

PEERDIR(
    contrib/libs/protobuf_std/builtin_proto/protos_from_protobuf
)

SRCS(
    google/protobuf/compiler/plugin.proto
)

END()

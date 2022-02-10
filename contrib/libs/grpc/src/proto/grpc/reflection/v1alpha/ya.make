PROTO_LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    akastornov
    g:contrib
    g:cpp-contrib
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/grpc
)

PY_NAMESPACE(src.proto.grpc.reflection.v1alpha) 
 
GRPC()

SRCS(
    reflection.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()

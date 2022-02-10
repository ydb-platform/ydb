PROTO_LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt) 
 
OWNER(
    akastornov
    g:contrib
    g:cpp-contrib
)

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
    PY3_PROTO
)

PROTO_NAMESPACE( 
    GLOBAL 
    contrib/libs/grpc 
) 

PEERDIR(
    contrib/libs/grpc/src/proto/grpc/testing
)

GRPC()

SRCS(
    echo_duplicate.proto
)

END()

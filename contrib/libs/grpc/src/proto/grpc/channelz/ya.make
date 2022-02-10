PROTO_LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt) 
 
OWNER(
    akastornov
    dvshkurko
    g:contrib
    g:cpp-contrib
)

PROTO_NAMESPACE( 
    GLOBAL 
    contrib/libs/grpc 
) 

GRPC()

SRCS(
    channelz.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()

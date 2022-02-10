PROTO_LIBRARY()

OWNER(
    ilnaz
    g:kikimr
)

SRCS(
    sensitive.proto
    validation.proto
)

EXCLUDE_TAGS(GO_PROTO) 
 
END()

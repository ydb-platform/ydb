PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

PROTO_NAMESPACE(yt)

PY_NAMESPACE(yt.library.ytprof.proto)

SRCS(
    profile.proto
)

END()

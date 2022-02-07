PROTO_LIBRARY()

OWNER(g:passport_infra)

EXCLUDE_TAGS(
    JAVA_PROTO
    PY_PROTO
    PY3_PROTO
)

SRCS(
    settings.proto
)

END()

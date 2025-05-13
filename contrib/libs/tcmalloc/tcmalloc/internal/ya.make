PROTO_LIBRARY()

VERSION(dummy)

LICENSE(Apache-2.0)

WITHOUT_LICENSE_TEXTS()

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
    JAVA_PROTO
)

SRC(profile.proto)

ADDINCL(
    GLOBAL contrib/libs/tcmalloc
)

END()

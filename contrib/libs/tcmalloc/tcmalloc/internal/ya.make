PROTO_LIBRARY()

VERSION(2025-02-22)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/7dd049e3367acff457a20cc4fb4c8b366cb2892d.tar.gz)

LICENSE(Apache-2.0)

WITHOUT_LICENSE_TEXTS()

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
    JAVA_PROTO
)

SRC(profile.proto)

END()

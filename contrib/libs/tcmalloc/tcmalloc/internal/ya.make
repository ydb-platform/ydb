PROTO_LIBRARY()

VERSION(2025-01-30)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/c8dfee3e4c489c5ae0d30c484c92db102a69ec51.tar.gz)

LICENSE(Apache-2.0)

WITHOUT_LICENSE_TEXTS()

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
    JAVA_PROTO
)

SRC(profile.proto)

END()

LIBRARY()

LICENSE(Apache-2.0)

WITHOUT_LICENSE_TEXTS()

VERSION(2025-01-30)

NO_COMPILER_WARNINGS()

SRCDIR(contrib/libs/tcmalloc/tcmalloc)

PEERDIR(
    contrib/libs/tcmalloc/tcmalloc/internal
    contrib/restricted/abseil-cpp
    contrib/libs/protobuf
)

SRCS(
    internal/profile_builder.cc
    profile_marshaler.cc
)

ADDINCL(
    GLOBAL contrib/libs/tcmalloc
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/tcmalloc
)

END()

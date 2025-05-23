SUBSCRIBER(
    g:cpp-contrib
    ayles
    mikailbag
)

LIBRARY()

WITHOUT_LICENSE_TEXTS()

SRCDIR(contrib/libs/tcmalloc/tcmalloc)

PEERDIR(
    contrib/libs/tcmalloc/tcmalloc/internal
    contrib/libs/protobuf
)

SRCS(
    internal/profile_builder.cc
    profile_marshaler.cc
)

END()

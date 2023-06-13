LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

ADDINCL(contrib/libs/highwayhash)

SRCDIR(contrib/libs/highwayhash/highwayhash)

CFLAGS(-msse4.1)

NO_COMPILER_WARNINGS()

SRCS(
    hh_sse41.cc
)

END()

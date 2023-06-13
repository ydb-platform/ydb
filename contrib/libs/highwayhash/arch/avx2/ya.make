LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

ADDINCL(contrib/libs/highwayhash)

SRCDIR(contrib/libs/highwayhash/highwayhash)

CFLAGS(-mavx2)

NO_COMPILER_WARNINGS()

SRCS(
    sip_tree_hash.cc
    hh_avx2.cc
)

END()

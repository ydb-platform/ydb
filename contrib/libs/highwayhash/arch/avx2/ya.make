LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(2017-05-08-2b666ae078292b01024453d01480f3b362a2a012)

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

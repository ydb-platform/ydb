LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2017-05-08-2b666ae078292b01024453d01480f3b362a2a012)

NO_COMPILER_WARNINGS()

ADDINCL(GLOBAL contrib/libs/highwayhash)

SRCDIR(contrib/libs/highwayhash/highwayhash)

SRCS(
    # Dispatcher
    arch_specific.cc
    instruction_sets.cc
    nanobenchmark.cc
    os_specific.cc
    # SipHash
    sip_hash.cc
    scalar_sip_tree_hash.cc
    # sip_tree_hash.cc with AVX2 if available
    # HighwayHash
    hh_portable.cc
    # hh_avx2.cc with AVX2
    # hh_sse41.cc with SSE4.1
    # Library
    c_bindings.cc
)

IF (ARCH_X86_64)
    PEERDIR(
        contrib/libs/highwayhash/arch/avx2
        contrib/libs/highwayhash/arch/sse41
    )
ELSE()
    SRCS(
        sip_tree_hash.cc
    )
ENDIF()

END()

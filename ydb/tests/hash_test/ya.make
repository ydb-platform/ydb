PROGRAM()

PEERDIR(
    ydb/tests/hash_test/xxHash
    ydb/core/blobstorage/crypto
    contrib/restricted/cityhash-1.0.2
    library/cpp/getopt
    ydb/library/actors/util
)

SRCS(
    main.cpp
    bench.cpp
    func.cpp
    variants.cpp
    kernel_mem.cpp
    kernel_chacha.cpp
    kernel_city.cpp
)

IF (NOT OS_WINDOWS)
    SRCS(
        kernel_xxh_scalar.cpp
    )
ENDIF()

IF (NOT OS_WINDOWS AND NOT ARCH_ARM64)
    SRCS(
        kernel_chacha_vec.cpp
        kernel_chacha512.cpp
        kernel_xxh_sse2.cpp
    )

    SRC(
        kernel_mem_avx2.cpp
        -mavx2
    )
    SRC(
        kernel_xxh_avx2.cpp
        -mavx2
    )
ENDIF()

IF (NOT OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        kernel_xxh_neon.cpp
    )
ENDIF()

END()

LIBRARY()

SRCS(
    mpmc_unordered_ring.cpp
    mpmc_unordered_ring.h
    mpsc_htswap.cpp
    mpsc_htswap.h
    mpsc_intrusive_unordered.cpp
    mpsc_intrusive_unordered.h
    mpsc_read_as_filled.cpp
    mpsc_read_as_filled.h
    mpsc_vinfarr_obstructive.cpp
    mpsc_vinfarr_obstructive.h
)

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)

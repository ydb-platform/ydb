UNITTEST()

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/scheme
    library/cpp/scheme/tests/fuzz_ops/lib
)

SRCS(
    vm_parse_ut.cpp
)

END()

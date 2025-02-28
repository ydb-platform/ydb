UNITTEST_FOR(library/cpp/digest/argonish)

PEERDIR(
    library/cpp/digest/argonish
)

SRCS(
    ut.cpp
)

TAG(
    sb:intel_e5_2660v4
    ya:fat
    ya:force_sandbox
    ya:large_tests_on_multi_slots
    ya:large_tests_on_ya_make_2
)

SIZE(LARGE)

END()

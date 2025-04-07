FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
    ya:large_tests_on_multi_slots
    ya:large_tests_on_ya_make_2
)

PEERDIR(
    library/cpp/containers/intrusive_rb_tree
)

SRCS(
    rb_tree_fuzzing.cpp
)

END()

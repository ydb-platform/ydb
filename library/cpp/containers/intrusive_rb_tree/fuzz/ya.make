FUZZ()

OWNER(
    g:util
    mikari
)

SIZE(LARGE)

TAG(ya:fat)

PEERDIR(
    library/cpp/containers/intrusive_rb_tree
)

SRCS(
    rb_tree_fuzzing.cpp
)

END()

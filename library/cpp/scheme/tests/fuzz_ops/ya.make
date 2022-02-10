FUZZ()

OWNER(
    g:blender
    g:middle
    g:upper
    velavokr
)

SRCS(
    fuzz_ops.cpp
)

PEERDIR(
    library/cpp/scheme/tests/fuzz_ops/lib 
)

END()

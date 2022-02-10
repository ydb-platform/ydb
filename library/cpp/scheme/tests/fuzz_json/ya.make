FUZZ()

OWNER(
    g:blender
    g:middle
    g:upper
    velavokr
)

SIZE(MEDIUM)

SRCS(
    fuzz_json.cpp
)

PEERDIR(
    library/cpp/scheme/tests/fuzz_json/lib 
)

END()

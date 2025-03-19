UNITTEST_FOR(library/cpp/json/yson)

ALLOCATOR(LF)

DATA(
    sbr://363537653
)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/histogram/simple
    library/cpp/testing/unittest
)

SIZE(LARGE)

TAG(
    ya:fat
    ya:large_tests_on_multi_slots
    ya:large_tests_on_ya_make_2
)

TIMEOUT(600)

SRCS(
    json2yson_ut.cpp
)

END()

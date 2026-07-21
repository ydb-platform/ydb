LIBRARY()

PEERDIR(
    library/cpp/json
    library/cpp/yson
    library/cpp/yson/json
)

SRCS(
    json2yson.cpp
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
    ut
)

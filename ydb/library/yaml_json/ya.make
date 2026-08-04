LIBRARY()

SRCS(
    yaml_to_json.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/yaml/as
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
)

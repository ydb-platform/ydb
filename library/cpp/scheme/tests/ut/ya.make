UNITTEST()


PEERDIR(
    library/cpp/protobuf/util
    library/cpp/scheme/tests/fuzz_ops/lib
    library/cpp/scheme/ut_utils
    library/cpp/string_utils/quote
    library/cpp/testing/unittest
)

SRCS(
    fuzz_ops_found_bugs_ut.cpp
    scheme_cast_ut.cpp
    scheme_json_ut.cpp
    scheme_merge_ut.cpp
    scheme_path_ut.cpp
    scheme_proto_ut.cpp
    scheme_ut.cpp
    scheme_ut.proto
)

END()

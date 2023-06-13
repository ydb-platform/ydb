LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/binsaver
    library/cpp/protobuf/util/proto
    library/cpp/string_utils/base64
)

SRCS(
    is_equal.cpp
    iterators.h
    merge.cpp
    path.cpp
    pb_io.cpp
    pb_utils.h
    repeated_field_utils.h
    simple_reflection.cpp
    walk.cpp
)

END()

RECURSE_FOR_TESTS(ut)

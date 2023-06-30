LIBRARY()

SRCS(
    type_info.cpp

    builder.cpp
    error.cpp
    type.cpp
    type_complexity.cpp
    type_equivalence.cpp
    type_factory.cpp
    type_io.cpp
    type_list.cpp
)

GENERATE_ENUM_SERIALIZATION(
    type_list.h
)

PEERDIR(
    library/cpp/yson_pull
)

END()

RECURSE_FOR_TESTS(ut)

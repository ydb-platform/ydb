UNITTEST()

SRCS(
    builder.cpp
    type_basics.cpp
    type_complexity_ut.cpp
    type_constraints.cpp
    type_deserialize.cpp
    type_equivalence.cpp
    type_factory.cpp
    type_factory_raw.cpp
    type_io.cpp
    type_list.cpp
    type_serialize.cpp
    type_show.cpp
    type_strip_tags.cpp
    test_data.cpp
)

PEERDIR(
    library/cpp/type_info
    library/cpp/yson
    library/cpp/yson/node
    library/cpp/resource
)

RESOURCE(
    ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data/good-types.txt /good
    ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data/bad-types.txt /bad
)

END()

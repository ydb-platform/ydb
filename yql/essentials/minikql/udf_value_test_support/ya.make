LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/public/decimal
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/utils/random_data_generator
)

SRCS(
    decimal_literal.cpp
    dynumber.cpp
    pg_int.cpp
    singular_null.cpp
    singular_void.cpp
    stream_view.cpp
    struct_type.cpp
    struct_variant_type.cpp
    tagged.cpp
    test_types_equal_to.cpp
    test_types_hash.cpp
    test_types_string_converter.cpp
    udf_value_comparator_utils.cpp
    utf8.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

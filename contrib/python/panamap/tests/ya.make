PY3TEST()

PEERDIR(
    contrib/python/panamap
)

TEST_SRCS(
    test_custom_descriptors.py
    test_map_dataclasses.py
    test_map_empty_classes.py
    test_map_iterables.py
    test_map_matching.py
    test_map_nested_classes.py
    test_map_primitive_classes.py
    test_map_primitive_values.py
    test_map_to_dict.py
    test_map_with_context.py
    test_map_with_custom_converter.py
)

NO_LINT()

END()

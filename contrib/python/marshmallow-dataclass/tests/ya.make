PY3TEST()

PEERDIR(
    contrib/python/marshmallow-dataclass
)

TEST_SRCS(
    test_attribute_copy.py
    test_city_building_examples.py
    test_class_schema.py
    test_doctests.py
    test_field_for_schema.py
    test_forward_references.py
    test_memory_leak.py
    test_optional.py
    test_post_load.py
    test_postdump.py
    test_union.py
)

RESOURCE_FILES(
    test_mypy.yml
)

NO_LINT()

END()

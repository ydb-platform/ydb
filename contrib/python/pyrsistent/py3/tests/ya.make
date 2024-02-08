PY3TEST()

PEERDIR(
    contrib/python/pyrsistent
)

TEST_SRCS(
    bag_test.py
    checked_map_test.py
    checked_set_test.py
    checked_vector_test.py
    class_test.py
    deque_test.py
    field_test.py
    freeze_test.py
    immutable_object_test.py
    list_test.py
    map_test.py
    record_test.py
    regression_test.py
    set_test.py
    toolz_test.py
)

NO_LINT()

END()

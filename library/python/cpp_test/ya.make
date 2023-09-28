PY3_LIBRARY()

PEERDIR(
    build/config/tests/cpp_style
    contrib/python/PyYAML
    library/python/resource
    library/python/testing/style
)

TEST_SRCS(
    conftest.py
    test_cpp.py
)

END()

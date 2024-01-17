PY23_LIBRARY()

TEST_SRCS(
    conftest.py
    test_common.py
    test_helpers.py
    test_typing.py
    test_io.py
    helpers.py
)

PEERDIR(
    library/python/cyson
    yt/python/yt/type_info
)

RESOURCE_FILES(
    library/cpp/type_info/ut/test-data/good-types.txt
    library/cpp/type_info/ut/test-data/bad-types.txt
)

END()

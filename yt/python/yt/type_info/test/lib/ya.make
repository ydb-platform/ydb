PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/type_info/test/lib)
ELSE()
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
ENDIF()

END()

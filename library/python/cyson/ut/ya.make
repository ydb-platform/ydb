PY23_TEST()

PEERDIR(
    library/python/cyson
)

IF(NOT OS_WINDOWS)
    PEERDIR(
        contrib/python/numpy
    )
ENDIF()

TEST_SRCS(
    test_control_attributes.py
    test_input_stream.py
    test_py_reader_writer.py
    test_reader_writer.py
    test_unsigned_long.py
)

END()

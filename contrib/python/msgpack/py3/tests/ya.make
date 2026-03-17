PY3TEST()

PEERDIR(contrib/python/msgpack)

TEST_SRCS(
    test_buffer.py
    test_case.py
    test_except.py
    test_extension.py
    test_format.py
    test_limits.py
    test_memoryview.py
    test_newspec.py
    test_obj.py
    test_pack.py
    test_read_size.py
    test_seq.py
    test_sequnpack.py
    test_stricttype.py
    test_subtype.py
    test_timestamp.py
    test_unpack.py
)

NO_LINT()

END()

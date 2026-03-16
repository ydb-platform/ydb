PY3TEST()

PEERDIR(
    contrib/python/partd
    contrib/python/numpy
    contrib/python/pandas
)

SRCDIR(contrib/python/partd/partd/tests)

TEST_SRCS(
    test_buffer.py
    test_compressed.py
    test_dict.py
    test_encode.py
    test_file.py
    test_numpy.py
    test_pandas.py
    test_partd.py
    test_pickle.py
    test_python.py
    test_utils.py
    test_zmq.py
)

NO_LINT()

END()

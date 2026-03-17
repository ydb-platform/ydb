PY3TEST()

PEERDIR(
    contrib/python/absl-py
    contrib/python/ml-dtypes
)

NO_LINT()

TEST_SRCS(
    custom_float_test.py
    finfo_test.py
    iinfo_test.py
    int4_test.py
    metadata_test.py
)

END()

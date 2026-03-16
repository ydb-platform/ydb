PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/hdbscan
    contrib/python/matplotlib
)

SRCDIR(
    contrib/python/hdbscan
)

TEST_SRCS(
    hdbscan/tests/__init__.py
    hdbscan/tests/test_branches.py
    hdbscan/tests/test_flat.py
    hdbscan/tests/test_hdbscan.py
    hdbscan/tests/test_prediction_utils.py
    hdbscan/tests/test_rsl.py
)

END()

PY2TEST()

PEERDIR(
    contrib/python/pandas
    contrib/python/statsmodels
    contrib/python/statsmodels/py2/statsmodels/datasets
)

NO_LINT()

SRCDIR(contrib/python/statsmodels/py2/statsmodels/tools/tests)

TEST_SRCS(
    test_catadd.py
    test_data.py
    test_decorators.py
    test_eval_measures.py
    test_grouputils.py
    test_linalg.py
    # test_numdiff.py
    test_parallel.py
    test_rootfinding.py
    test_sequences.py
    test_testing.py
    # test_tools.py
    test_transform_model.py
    test_web.py
)

END()

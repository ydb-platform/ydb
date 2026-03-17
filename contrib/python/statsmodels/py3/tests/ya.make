PY3TEST()

PEERDIR(
    contrib/python/pandas
    contrib/python/statsmodels
)

NO_LINT()

SRCDIR(contrib/python/statsmodels/py3/statsmodels)

TEST_SRCS(
    conftest.py
    tests/__init__.py
    # tests/test_package.py
    tools/tests/__init__.py
    tools/tests/test_catadd.py
    tools/tests/test_data.py
    tools/tests/test_decorators.py
    tools/tests/test_docstring.py
    tools/tests/test_eval_measures.py
    tools/tests/test_grouputils.py
    tools/tests/test_linalg.py
    # tools/tests/test_numdiff.py
    tools/tests/test_parallel.py
    tools/tests/test_rootfinding.py
    tools/tests/test_sequences.py
    tools/tests/test_testing.py
    # tools/tests/test_tools.py
    tools/tests/test_transform_model.py
    tools/tests/test_web.py
    tools/validation/tests/__init__.py
    tools/validation/tests/test_validation.py

)

END()

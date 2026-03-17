PY3TEST()

SIZE(MEDIUM)

FORK_TESTS()

PEERDIR(
    contrib/python/hyperopt
    contrib/deprecated/python/nose
    contrib/python/pymongo
)

SRCDIR(contrib/python/hyperopt/hyperopt)

TEST_SRCS(
    pyll/tests/__init__.py
    pyll/tests/test_base.py
    pyll/tests/test_stochastic.py
    tests/__init__.py
    # tests/integration/__init__.py
    # tests/integration/test_mongoexp.py
    # tests/integration/test_spark.py
    tests/test_anneal.py
    # tests/test_atpe_basic.py
    tests/test_base.py
    tests/test_criteria.py
    tests/test_domains.py
    tests/test_fmin.py
    tests/test_ipy.py
    # tests/test_mongoexp.py
    tests/test_pchoice.py
    tests/test_plotting.py
    tests/test_progress.py
    tests/test_pyll_utils.py
    tests/test_rand.py
    tests/test_randint.py
    # tests/test_rdists.py
    # tests/test_spark.py
    tests/test_tpe.py
    tests/test_utils.py
    tests/test_vectorize.py
    tests/test_webpage.py
    tests/unit/__init__.py
    tests/unit/test_anneal.py
    # tests/unit/test_atpe_basic.py
    tests/unit/test_criteria.py
    tests/unit/test_domains.py
    tests/unit/test_fmin.py
    tests/unit/test_ipy.py
    tests/unit/test_pchoice.py
    tests/unit/test_plotting.py
    tests/unit/test_progress.py
    tests/unit/test_pyll_utils.py
    tests/unit/test_rand.py
    tests/unit/test_randint.py
    # tests/unit/test_rdists.py
    # tests/unit/test_tpe.py
    tests/unit/test_utils.py
    tests/unit/test_vectorize.py
    tests/unit/test_webpage.py
)

NO_LINT()

END()

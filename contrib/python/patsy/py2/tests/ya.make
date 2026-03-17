PY2TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/patsy
    contrib/python/six
)

NO_LINT()

SRCDIR(
    contrib/python/patsy/py2/patsy
)

TEST_SRCS(
    test_build.py
    test_highlevel.py
    test_regressions.py
    test_splines_bs_data.py
    test_splines_crs_data.py
    test_state.py
)

END()

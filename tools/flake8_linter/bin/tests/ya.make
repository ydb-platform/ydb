PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_flake8_ver.py
    test_migrations.py
    test_noqa.py
    test_report.py
    util.py
)

PEERDIR(
    build/plugins/lib
    contrib/python/mergedeep
    devtools/ya/test/tests/lib/common
    library/python/testing/custom_linter_util
)

DEPENDS(
    tools/flake8_linter/bin
    tools/flake8_linter/bin/tests/stub
)

END()

RECURSE(
    stub
)

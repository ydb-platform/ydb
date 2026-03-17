PY3TEST()

SRCDIR(contrib/python/mkdocs)

TEST_SRCS(
    mkdocs/tests/__init__.py
    # mkdocs/tests/babel_cmd_tests.py
    mkdocs/tests/build_tests.py
    # mkdocs/tests/cli_tests.py
    mkdocs/tests/config/__init__.py
    mkdocs/tests/config/base_tests.py
    mkdocs/tests/config/config_options_tests.py
    mkdocs/tests/config/config_tests.py
    mkdocs/tests/conftest.py
    # mkdocs/tests/gh_deploy_tests.py
    mkdocs/tests/integration.py
    # mkdocs/tests/livereload_tests.py
    mkdocs/tests/localization_tests.py
    mkdocs/tests/new_tests.py
    mkdocs/tests/plugin_tests.py
    mkdocs/tests/search_tests.py
    mkdocs/tests/structure/__init__.py
    mkdocs/tests/structure/file_tests.py
    mkdocs/tests/structure/nav_tests.py
    mkdocs/tests/structure/page_tests.py
    mkdocs/tests/structure/toc_tests.py
    mkdocs/tests/theme_tests.py
    mkdocs/tests/utils/__init__.py
    mkdocs/tests/utils/babel_stub_tests.py
    mkdocs/tests/utils/utils_tests.py
)

PY_SRCS(
    TOP_LEVEL
    mkdocs/tests/base.py
)

PEERDIR(
    contrib/python/Babel
    contrib/python/mkdocs
)

DEPENDS(
    contrib/python/mkdocs/bin
)

DATA(
    arcadia/contrib/python/mkdocs/mkdocs/tests
    sbr://2915628787=docs
)

NO_LINT()

END()

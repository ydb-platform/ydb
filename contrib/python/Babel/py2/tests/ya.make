PY2TEST()

PEERDIR(
    contrib/python/Babel
    contrib/python/freezegun
)

DATA(
    arcadia/contrib/python/Babel/py2/babel
)

FORK_SUBTESTS()

TEST_SRCS(
    __init__.py
    conftest.py
    test_core.py
    test_date_intervals.py
    test_dates.py
    test_day_periods.py
    test_languages.py
    test_lists.py
    test_localedata.py
    messages/__init__.py
    messages/test_catalog.py
    messages/test_checkers.py
    messages/test_extract.py
    # messages/test_frontend.py
    messages/test_js_extract.py
    messages/test_jslexer.py
    messages/test_mofile.py
    messages/test_normalized_string.py
    messages/test_plurals.py
    messages/test_pofile.py
    test_numbers.py
    test_plural.py
    test_smoke.py
    test_support.py
    test_util.py
)

NO_LINT()

END()

PY2TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/python/convertdate
    contrib/python/dateparser
    contrib/python/mock
    contrib/python/parameterized
    contrib/python/six
)

PY_SRCS(
    NAMESPACE tests
    __init__.py
)

TEST_SRCS(
    test_clean_api.py
    test_data.py
    test_date.py
    test_date_parser.py
    # test_dateparser_data_integrity.py
    test_freshness_date_parser.py
    # test_hijri.py
    test_jalali.py
    test_languages.py
    test_loading.py
    test_parser.py
    test_pickle.py
    test_search.py
    test_settings.py
    test_timezone_parser.py
    test_utils.py
    test_utils_strptime.py
)

NO_LINT()

END()

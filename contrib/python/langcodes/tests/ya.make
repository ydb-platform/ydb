PY3TEST()

PEERDIR(
    contrib/python/langcodes
    contrib/python/language-data
)

NO_LINT()

SRCDIR(contrib/python/langcodes)

TEST_SRCS(
    langcodes/tests/test_alpha3.py
    langcodes/tests/test_issue_59.py
    langcodes/tests/test_language.py
    langcodes/tests/test_language_data.py
    langcodes/tests/test_wikt_languages.py
)

END()

PY3TEST()

PEERDIR(
    contrib/python/trafilatura
    contrib/python/requests
)

NO_LINT()

SIZE(MEDIUM)

FORK_TEST_FILES()

TEST_SRCS(
    __init__.py
    baseline_tests.py
    cli_tests.py
    deduplication_tests.py
    downloads_tests.py
    feeds_tests.py
    filters_tests.py
    json_metadata_tests.py
    metadata_tests.py
    sitemaps_tests.py
    spider_tests.py
    unit_tests.py
    xml_tei_tests.py
    utils.py
)

DATA(
    arcadia/contrib/python/trafilatura/tests/resources
)

END()

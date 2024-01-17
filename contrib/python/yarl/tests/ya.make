PY3TEST()

PEERDIR(
    contrib/python/yarl
)

TEST_SRCS(
    test_cache.py
    test_cached_property.py
    test_normalize_path.py
    test_pickle.py
    test_quoting.py
    test_update_query.py
    test_url.py
    test_url_build.py
    test_url_cmp_and_hash.py
    test_url_parsing.py
    test_url_query.py
    test_url_update_netloc.py
)

NO_LINT()

END()

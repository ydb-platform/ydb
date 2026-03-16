PY3TEST()

PEERDIR(
    contrib/python/betamax
    contrib/python/pytest
    contrib/python/mock
)

DATA(
    arcadia/contrib/python/betamax/tests/cassettes
)

TEST_SRCS(
    conftest.py
    unit/test_adapter.py
    unit/test_betamax.py
    unit/test_cassette.py
    unit/test_configure.py
    unit/test_decorator.py
    unit/test_exceptions.py
    unit/test_fixtures.py
    unit/test_matchers.py
    unit/test_options.py
    unit/test_recorder.py
    unit/test_replays.py
    unit/test_serializers.py
    # regression/test_can_replay_interactions_multiple_times.py
    # regression/test_cassettes_retain_global_configuration.py
    # regression/test_gzip_compression.py
    # regression/test_once_prevents_new_interactions.py
    # regression/test_requests_2_11_body_matcher.py
    # regression/test_works_with_digest_auth.py
)

NO_LINT()

END()

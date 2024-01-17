PY3TEST()

PEERDIR(
    contrib/python/requests-oauthlib
    contrib/python/requests-mock
)

# These tests use real http://httpbin.org that is why they are disabled:
# testCanPostBinaryData
# test_url_is_native_str
# test_content_type_override

TEST_SRCS(
    __init__.py
    test_compliance_fixes.py
    test_core.py
    test_oauth1_session.py
    test_oauth2_auth.py
    test_oauth2_session.py
)

DATA(
    arcadia/contrib/python/requests-oauthlib/tests
)

NO_LINT()

END()

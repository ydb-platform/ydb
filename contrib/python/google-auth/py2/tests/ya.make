PY2TEST()

PEERDIR(
    contrib/python/Flask
    contrib/python/google-auth
    contrib/python/mock
    contrib/python/responses
    contrib/python/pyOpenSSL
    contrib/python/pytest-localserver
    contrib/python/oauth2client
    contrib/python/freezegun
)

DATA(
    arcadia/contrib/python/google-auth/py2/tests/data
)

PY_SRCS(
    NAMESPACE tests
    transport/__init__.py
    transport/compliance.py
)

TEST_SRCS(
    __init__.py
    compute_engine/__init__.py
    compute_engine/test__metadata.py
    compute_engine/test_credentials.py
    conftest.py
    crypt/__init__.py
    crypt/test__cryptography_rsa.py
    crypt/test__python_rsa.py
    crypt/test_crypt.py
    crypt/test_es256.py
    oauth2/__init__.py
    oauth2/test__client.py
    # oauth2/test_challenges.py - need pyu2f
    oauth2/test_credentials.py
    oauth2/test_id_token.py
    oauth2/test_reauth.py
    oauth2/test_service_account.py
    oauth2/test_sts.py
    oauth2/test_utils.py
    test__cloud_sdk.py
    test__default.py
    test__helpers.py
    test__oauth2client.py
    test__service_account_info.py
    test_app_engine.py
    test_aws.py
    test_credentials.py
    test_downscoped.py
    test_external_account.py
    test_iam.py
    test_identity_pool.py
    test_impersonated_credentials.py
    test_jwt.py
    transport/test__http_client.py
    transport/test__mtls_helper.py
    transport/test_grpc.py
    transport/test_mtls.py
    # transport/test_requests.py
    # transport/test_urllib3.py
)

RESOURCE(
    data/privatekey.pem data/privatekey.pem
    data/public_cert.pem data/public_cert.pem
)

NO_LINT()

END()

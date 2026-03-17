PY2TEST()

PEERDIR(
    contrib/python/validators
)

TEST_SRCS(
    __init__.py
    i18n/__init__.py
    i18n/test_es.py
    i18n/test_fi.py
    test_between.py
    test_domain.py
    test_email.py
    test_extremes.py
    test_iban.py
    test_ipv4.py
    test_ipv4_cidr.py
    test_ipv6.py
    test_ipv6_cidr.py
    test_length.py
    test_mac_address.py
    test_md5.py
    test_sha1.py
    test_sha224.py
    test_sha256.py
    test_sha512.py
    test_slug.py
    test_url.py
    test_uuid.py
    test_validation_failure.py
)

NO_LINT()

END()

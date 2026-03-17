PY3TEST()

PEERDIR(
    contrib/python/M2Crypto
    contrib/python/parameterized
)

NO_LINT()

PY_SRCS(
    NAMESPACE tests
    __init__.py
    fips.py
    test_ssl.py
)

# XXX: These tests do stuff like reading/writing files, starting servers,
# breaking ya make tests node...
# Only those playing well are enabled, others require some intrusive changes to work

TEST_SRCS(
    test_aes.py
    test_asn1.py
    test_authcookie.py
    test_bio.py
#    test_bio_file.py
#    test_bio_iobuf.py
    test_bio_membuf.py
#    test_bio_ssl.py
    test_bn.py
#    test_dh.py
#    test_dsa.py
#    test_ec_curves.py
#    test_ecdh.py
#    test_ecdsa.py
#    test_engine.py
#    test_evp.py
#    test_obj.py
#    test_rand.py
    test_rc4.py
#    test_rsa.py
#    test_smime.py
#    test_ssl.py
#    test_ssl_offline.py
    test_ssl_win.py
    test_threading.py
    test_timeout.py
    test_util.py
#    test_x509.py
)

END()

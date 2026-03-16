PY3TEST()

PEERDIR(
    contrib/python/fido2
)

TEST_SRCS(
    hid/__init__.py
    hid/test_base.py
    __init__.py
    test_attestation.py
    test_cbor.py
    test_client.py
    test_cose.py
    test_ctap1.py
    test_ctap2.py
    test_hid.py
    test_mds3.py
    test_pcsc.py
    test_rpid.py
    test_server.py
    test_tpm.py
    test_utils.py
    test_webauthn.py
    test_webauthn_legacy_mapping.py
    utils.py
)

NO_LINT()

END()

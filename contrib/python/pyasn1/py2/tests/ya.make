PY2TEST()

PEERDIR(
    contrib/python/pyasn1
)

TEST_SRCS(
    __init__.py
    base.py
    codec/__init__.py
    codec/ber/__init__.py
    codec/ber/test_decoder.py
    codec/ber/test_encoder.py
    codec/cer/__init__.py
    codec/cer/test_decoder.py
    codec/cer/test_encoder.py
    codec/der/__init__.py
    codec/der/test_decoder.py
    codec/der/test_encoder.py
    codec/native/__init__.py
    codec/native/test_decoder.py
    codec/native/test_encoder.py
    codec/test_streaming.py
    compat/__init__.py
    compat/test_integer.py
    compat/test_octets.py
    test_debug.py
    type/__init__.py
    type/test_char.py
    type/test_constraint.py
    type/test_namedtype.py
    type/test_namedval.py
    type/test_opentype.py
    type/test_tag.py
    type/test_univ.py
    type/test_useful.py
)

NO_LINT()

END()

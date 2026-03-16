PY3TEST()

PEERDIR(
    contrib/python/cbor2
    contrib/python/hypothesis
)

NO_LINT()

TEST_SRCS(
    conftest.py
    hypothesis_strategies.py
    test_decoder.py
    test_encoder.py
    test_tool.py
    test_types.py
)

DATA(
    arcadia/contrib/python/cbor2/tests
)

END()

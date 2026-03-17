PY2TEST()

PEERDIR(
    contrib/python/marshmallow-enum
)

TEST_SRCS(
    test_enum_field.py
)

NO_LINT()

END()

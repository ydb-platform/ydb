PY3TEST()

SIZE(MEDIUM)

BUILD_ONLY_IF(WARNING OS_LINUX OS_DARWIN)
IF (OS_LINUX)
    DATA(sbr://320653966)
ELSEIF (OS_DARWIN)
    DATA(sbr://769724493)
ENDIF()

PEERDIR(
    contrib/python/marshmallow-mongoengine
    library/python/pytest-mongodb
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_fields.py
    test_marshmallow_mongoengine.py
    test_params.py
    test_skip.py
)

NO_LINT()

END()

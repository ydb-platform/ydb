PY23_TEST()

SIZE(MEDIUM)

PEERDIR(
    library/python/pytest-mongodb
)

IF (PYTHON2)
    PEERDIR(contrib/python/pymongo/py2)
ELSE()
    PEERDIR(contrib/deprecated/python/pymongo)
ENDIF()

BUILD_ONLY_IF(WARNING OS_LINUX OS_DARWIN)
IF (OS_LINUX)
    DATA(sbr://320653966)
ELSEIF (OS_DARWIN)
    DATA(sbr://769724493)
ENDIF()

TEST_SRCS(test.py)

END()

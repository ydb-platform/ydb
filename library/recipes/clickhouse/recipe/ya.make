IF (NOT OS_WINDOWS)

PY3_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    contrib/python/requests

    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
)

END()

ENDIF()

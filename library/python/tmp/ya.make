PY23_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    library/python/fs
    library/python/unique_id
)

END()

RECURSE_FOR_TESTS(test)

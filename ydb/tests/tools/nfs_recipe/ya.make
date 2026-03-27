PY3_PROGRAM(nfs_recipe)

PY_SRCS(__main__.py)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()

RECURSE(
    fake_nfs
)

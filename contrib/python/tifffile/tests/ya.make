PY3TEST()

SIZE(MEDIUM)

FORK_TESTS()

ENV(SKIP_LARGE=1)
ENV(SKIP_HTTP=1)

PEERDIR(
    contrib/python/dask
    contrib/python/fsspec
    contrib/python/lxml
    contrib/python/tifffile
    contrib/python/zarr
)

ALL_PYTEST_SRCS()

NO_LINT()

END()

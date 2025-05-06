RECURSE(binaries)

PY23_LIBRARY()


PY_SRCS(
    fixtures.py
)

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-24-4/release/ydbd ydbd-prelast-stable
    OUT_NOAUTO ydbd-prelast-stable
)

END()

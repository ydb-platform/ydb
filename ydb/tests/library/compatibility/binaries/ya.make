RECURSE(downloader)

UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/binaries/downloader stable-25-1/release/ydbd ydbd-last-stable
    OUT_NOAUTO ydbd-last-stable
)

RUN_PROGRAM(
    ydb/tests/library/compatibility/binaries/downloader stable-24-4/release/ydbd ydbd-prelast-stable
    OUT_NOAUTO ydbd-prelast-stable
)

END()

UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-25-1/release/ydbd ydbd-last-stable
    OUT_NOAUTO ydbd-last-stable
)

END()

RECURSE(downloader)

UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-24-4/release/ydbd ydbd-last-stable
    OUT_NOAUTO ydbd-last-stable
)

END()

RECURSE(configs)
RECURSE(downloader)

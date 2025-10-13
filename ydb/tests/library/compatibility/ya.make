UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-25-1/release/ydbd ydbd-25-1
    OUT_NOAUTO ydbd-25-1
)

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-24-4/release/ydbd ydbd-24-4
    OUT_NOAUTO ydbd-24-4
)

END()

RECURSE(downloader)
RECURSE(configs)

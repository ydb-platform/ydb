UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-24-3/relwithdebinfo/ydbd ydbd-last-stable
    OUT_NOAUTO ydbd-last-stable
)

END()

RECURSE(downloader)

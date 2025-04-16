UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-25-1-1/release/ydbd ydbd-last-stable stable-24-4-4-hotfix/release/ydbd ydbd-prev-stable
    OUT_NOAUTO ydbd-last-stable ydbd-prev-stable
)

END()

RECURSE(downloader)

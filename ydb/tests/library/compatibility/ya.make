UNION()


RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-25-1/release/ydbd ydbd-last-stable
    OUT_NOAUTO ydbd-last-stable
)
RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-24-4/release/ydbd ydbd-prev-stable
    OUT_NOAUTO ydbd-prev-stable
)



END()

RECURSE(downloader)

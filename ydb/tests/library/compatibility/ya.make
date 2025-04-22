UNION()


RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-25-1/release/ydbd ydbd-last2-stable
    OUT_NOAUTO ydbd-last2-stable
)
RUN_PROGRAM(
    ydb/tests/library/compatibility/downloader stable-24-4/release/ydbd ydbd-prev2-stable
    OUT_NOAUTO ydbd-prev2-stable
)



END()

RECURSE(downloader)

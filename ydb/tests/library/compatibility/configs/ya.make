RECURSE(dump)
RECURSE(comparator)

UNION()

RUN_PROGRAM(
   ydb/tests/library/compatibility/binaries/downloader download stable-25-1/release/config-meta.json stable-25-1
   OUT_NOAUTO stable-25-1
)

RUN_PROGRAM(
   ydb/tests/library/compatibility/binaries/downloader download stable-25-1-1/release/config-meta.json stable-25-1-1
   OUT_NOAUTO stable-25-1-1
)

RUN_PROGRAM(
   ydb/tests/library/compatibility/binaries/downloader download stable-25-1-2/release/config-meta.json stable-25-1-2
   OUT_NOAUTO stable-25-1-2
)

RUN_PROGRAM(
       download stable-25-1-3/release/config-meta.json stable-25-1-3
   OUT_NOAUTO stable-25-1-3
)

RUN_PROGRAM(
  ydb/tests/library/compatibility/binaries/downloader download prestable-25-2/release/config-meta.json prestable-25-2
  OUT_NOAUTO prestable-25-2
)

RUN_PROGRAM(
   ydb/tests/library/compatibility/binaries/downloader download prestable-25-3/release/config-meta.json prestable-25-3
   OUT_NOAUTO prestable-25-3
)

RUN_PROGRAM(
    ydb/tests/library/compatibility/configs/dump/dumper
    STDOUT_NOAUTO current
)
RUN(
    echo current
    STDOUT_NOAUTO current-name
)

END()

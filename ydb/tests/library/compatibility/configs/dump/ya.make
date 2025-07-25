RECURSE(dumper)

UNION()

RUN_PROGRAM(
    ydb/tests/library/compatibility/configs/dump/dumper
    STDOUT_NOAUTO config-meta.json
)

END()

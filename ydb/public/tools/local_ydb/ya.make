PY3_PROGRAM(local_ydb)

PY_SRCS(
    __main__.py
)

RESOURCE_FILES(
    PREFIX ydb/public/tools/local_ydb/
    resources/minimal_yaml.yml
)

PEERDIR(
    yql/essentials/providers/common/proto
    ydb/public/tools/lib/cmds
)

END()

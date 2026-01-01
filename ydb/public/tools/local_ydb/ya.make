PY3_PROGRAM(local_ydb)

PY_SRCS(
    __main__.py
    resources/__init__.py
)

RESOURCE(
    resources/minimal_yaml.yml /minimal_yaml.yml
)

PEERDIR(
    yql/essentials/providers/common/proto
    ydb/public/tools/lib/cmds
)

END()

UNITTEST_FOR(ydb/public/lib/ydb_cli/dump)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRC(restore_compat_ut.cpp)

PEERDIR(
    ydb/public/lib/ydb_cli/dump
)

END()

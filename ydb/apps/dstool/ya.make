PY3_PROGRAM(ydb-dstool)
REQUIREMENTS(cpu:2)


STRIP()

PY_MAIN(ydb.apps.dstool.main)

PY_SRCS(
    main.py
)

PEERDIR(
    ydb/apps/dstool/lib
    contrib/python/six
)

RESOURCE(
    ydb/apps/dstool/version.txt version.txt
)

END()

RECURSE(lib)

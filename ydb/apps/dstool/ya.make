PY3_PROGRAM(ydb-dstool)

STRIP()

#
# DON'T ALLOW NEW DEPENDENCIES WITHOUT EXPLICIT APPROVE FROM  kikimr-dev@ or fomichev@
#
IF (OPENSOURCE)
    CHECK_DEPENDENT_DIRS(
        ALLOW_ONLY
        PEERDIRS
        arc/api/public
        build/external_resources/flake8_py3
        build/platform
        certs
        contrib
        library
        tools/archiver
        tools/enum_parser/enum_parser
        tools/enum_parser/enum_serialization_runtime
        tools/rescompressor
        tools/rorescompiler
        util
        ydb
    )
ENDIF()

PY_MAIN(ydb.apps.dstool.main)

PY_SRCS(
    main.py
)

PEERDIR(
    ydb/apps/dstool/lib
)

END()

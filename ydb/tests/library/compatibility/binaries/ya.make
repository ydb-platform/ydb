RECURSE(downloader)

UNION()

IF(NOT ${YDB_COMPAT_INIT_REF})
    SET(YDB_COMPAT_INIT_REF stable-25-1-1)
ENDIF()
IF(NOT ${YDB_COMPAT_INTER_REF})
    SET(YDB_COMPAT_INTER_REF stable-25-1-3)
ENDIF()
IF(NOT ${YDB_COMPAT_TARGET_REF})
    SET(YDB_COMPAT_TARGET_REF current)
ENDIF()

RUN_PROGRAM(
    ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_INTER_REF/release/ydbd ydbd-inter $YDB_COMPAT_INTER_REF
    OUT_NOAUTO ydbd-inter ydbd-inter-name
)

RUN_PROGRAM(
    ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_INIT_REF/release/ydbd ydbd-init $YDB_COMPAT_INIT_REF
    OUT_NOAUTO ydbd-init ydbd-init-name
)

IF(${YDB_COMPAT_TARGET_REF} != "current")
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_TARGET_REF/release/ydbd ydbd-target $YDB_COMPAT_TARGET_REF
        OUT_NOAUTO ydbd-target ydbd-target-name
    )
ELSE()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    BUNDLE(
        ydb/apps/ydbd NAME ydbd-target
    )
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader append-version ydbd-target-name current
        OUT_NOAUTO ydbd-target-name
    )
ENDIF()

END()

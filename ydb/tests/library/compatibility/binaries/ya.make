RECURSE(downloader)

UNION()

IF(NOT ${YDB_COMPAT_INIT_REF})
    SET(YDB_COMPAT_INIT_REF stable-24-4)
ENDIF()
IF(NOT ${YDB_COMPAT_INTER_REF})
    SET(YDB_COMPAT_INTER_REF stable-25-1-2)
ENDIF()
IF(NOT ${YDB_COMPAT_TARGET_REF})
    SET(YDB_COMPAT_TARGET_REF current)
ENDIF()

RUN_PROGRAM(
    ydb/tests/library/compatibility/binaries/downloader $YDB_COMPAT_INTER_REF/release/ydbd ydbd-inter-stable $YDB_COMPAT_INTER_REF
    OUT_NOAUTO ydbd-inter-stable ydbd-inter-stable-name
)

RUN_PROGRAM(
    ydb/tests/library/compatibility/binaries/downloader $YDB_COMPAT_INIT_REF/release/ydbd ydbd-init-stable $YDB_COMPAT_INIT_REF
    OUT_NOAUTO ydbd-init-stable ydbd-init-stable-name
)

IF(${YDB_COMPAT_TARGET_REF} != "current")
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader $YDB_COMPAT_TARGET_REF/release/ydbd ydbd-target-stable $YDB_COMPAT_TARGET_REF
        OUT_NOAUTO ydbd-target-stable ydbd-target-stable-name
    )
ENDIF()

END()

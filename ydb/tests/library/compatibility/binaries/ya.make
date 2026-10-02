RECURSE(downloader)

UNION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/compatibility/versions.inc)

IF(SANITIZER_TYPE == "address")
    SET(YDB_SAN_TYPE "-asan")
ENDIF()
# Not supported yet
# Falling back to unsanitized binaries (prevents configuration errors for nightly tests)
# ELSEIF(SANITIZER_TYPE == "memory")
#    SET(YDB_SAN_TYPE "-msan")
#ELSEIF(SANITIZER_TYPE == "thread")
#    SET(YDB_SAN_TYPE "-tsan")
#ENDIF()

IF(BUILD_TYPE == "RELEASE")
    SET(YDB_BUILD_TYPE "release")
ELSEIF(BUILD_TYPE == "DEBUG")
    SET(YDB_BUILD_TYPE "debug")
ELSEIF(BUILD_TYPE == "RELWITHDEBINFO")
    SET(YDB_BUILD_TYPE "relwithdebinfo")
ENDIF()

SET(YDB_BUILD_CONFIG ${YDB_BUILD_TYPE}${YDB_SAN_TYPE})

IF(${YDB_COMPAT_INTER_REF} != "current")
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_INTER_REF/${YDB_BUILD_CONFIG}/ydbd ydbd-inter $YDB_COMPAT_INTER_REF
        OUT_NOAUTO ydbd-inter ydbd-inter-name
    )
ELSE()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
    BUNDLE(
        ydb/apps/ydbd NAME ydbd-inter
    )
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader append-version ydbd-inter-name current
        OUT_NOAUTO ydbd-inter-name
    )
ENDIF()

IF(${YDB_COMPAT_INIT_REF} != "current")
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_INIT_REF/${YDB_BUILD_CONFIG}/ydbd ydbd-init $YDB_COMPAT_INIT_REF
        OUT_NOAUTO ydbd-init ydbd-init-name
    )
ELSE()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
    BUNDLE(
        ydb/apps/ydbd NAME ydbd-init
    )
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader append-version ydbd-init-name current
        OUT_NOAUTO ydbd-init-name
    )
ENDIF()

IF(${YDB_COMPAT_TARGET_REF} != "current")
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_TARGET_REF/${YDB_BUILD_CONFIG}/ydbd ydbd-target $YDB_COMPAT_TARGET_REF
        OUT_NOAUTO ydbd-target ydbd-target-name
    )
ELSE()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
    BUNDLE(
        ydb/apps/ydbd NAME ydbd-target
    )
    RUN_PROGRAM(
        ydb/tests/library/compatibility/binaries/downloader append-version ydbd-target-name current
        OUT_NOAUTO ydbd-target-name
    )
ENDIF()

END()

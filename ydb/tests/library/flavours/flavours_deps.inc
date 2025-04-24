DEPENDS(
    ydb/apps/ydbd
)

ENV(YDB_FLAVOUR_YDB="ydb/apps/ydbd/ydbd")

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/kikimr/tests/flavours/flavours_deps.inc)
ENDIF()

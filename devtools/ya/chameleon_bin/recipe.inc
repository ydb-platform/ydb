DEPENDS(devtools/ya/chameleon_bin)

INCLUDE(${ARCADIA_ROOT}/devtools/ya/opensource.inc)

IF (PYTHON3)
    DEPENDS(devtools/ya/bin3)
ELSE()
    IF (NOT OPENSOURCE)
        DEPENDS(devtools/ya/bin)
    ENDIF()
ENDIF()

IF (NEBIUS)
    DATA(
        arcadia/nebius/devtools/ya-bin/ya.conf
    )
ELSEIF (OPENSOURCE)
    DATA(
        arcadia/devtools/ya/opensource/ya.conf
    )
ELSE()
    DATA(
        arcadia/ya.conf
    )
ENDIF()

DATA(
    arcadia/ya
)

USE_RECIPE(devtools/ya/chameleon_bin/chameleon_bin ${PYTHON3})


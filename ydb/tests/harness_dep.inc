# This file incapsulates logic for ydbd dependency in tests

IF (DEFINED YDB_DRIVER_BINARY_PREBUILT)  # useful to test arbitrary prebuilt ydbd
    SET(YDB_DRIVER_BINARY ${YDB_DRIVER_BINARY_PREBUILT})
ELSEIF(DEFINED YDB_DRIVER_BINARY)  # custom in-source ydbd
    SET(YDB_DRIVER_BINARY ${YDB_DRIVER_BINARY})
    DEPENDS(
        ${YDB_DRIVER_BINARY}/..
    )
ELSE()  # default ydbd from current source tree
    SET(YDB_DRIVER_BINARY "ydb/apps/ydbd/ydbd")
    DEPENDS(
        ydb/apps/ydbd
    )
ENDIF()

ENV(YDB_DRIVER_BINARY=${YDB_DRIVER_BINARY})

DEPENDS(
    ydb/apps/ydb
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        ${ARCADIA_ROOT}/ydb/tests/tsan.supp
    )
ENDIF()
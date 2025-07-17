# This file incapsulates logic for ydbd dependency in tests

IF (DEFINED YDB_DRIVER_BINARY_PREBUILT)  # useful to test arbitrary prebuilt ydbd
    SET(YDB_DRIVER_BINARY ${YDB_DRIVER_BINARY_PREBUILT})
ELSEIF(DEFINED YDB_DRIVER_BINARY)  # custom in-source ydbd
    SET(YDB_DRIVER_BINARY ${YDB_DRIVER_BINARY})
    DEPENDS(
        ${YDB_DRIVER_BINARY}/..
    )
ELSE()  # default ydbd from current source tree
    SET(YDB_DRIVER_BINARY "ydb/tests/library/compatibility/binaries/ydbd-inter")
    SET(YDB_DRIVER_BINARY_OLD "ydb/tests/library/compatibility/binaries/ydbd-init")
    DEPENDS(
        ydb/tests/library/compatibility/binaries
    )
ENDIF()

ENV(YDB_DRIVER_BINARY=${YDB_DRIVER_BINARY})
ENV(YDB_DRIVER_BINARY_OLD=${YDB_DRIVER_BINARY_OLD})

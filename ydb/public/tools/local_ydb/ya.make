PY3_PROGRAM(local_ydb)

IF (OPENSOURCE)

    RESTRICT_LICENSES(
        DENY REQUIRE_DISCLOSURE FORBIDDEN PROTESTWARE
        # DTCC-553
        EXCEPT contrib/libs/linux-headers
    )

ENDIF()

PY_SRCS(__main__.py)

PEERDIR(
    ydb/public/tools/lib/cmds
)

END()

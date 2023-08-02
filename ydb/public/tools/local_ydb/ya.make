PY3_PROGRAM(local_ydb)

IF (OPENSOURCE)

    LICENSE_RESTRICTION(
        DENY REQUIRE_DISCLOSURE FORBIDDEN PROTESTWARE
    )

    LICENSE_RESTRICTION_EXCEPTIONS(
        contrib/libs/linux-headers  # DTCC-553
    )

ENDIF()

PY_SRCS(__main__.py)

PEERDIR(
    ydb/public/tools/lib/cmds
)

END()

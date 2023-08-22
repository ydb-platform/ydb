IF (OPENSOURCE)
    LICENSE_RESTRICTION(
        DENY REQUIRE_DISCLOSURE FORBIDDEN PROTESTWARE
    )
    LICENSE_RESTRICTION_EXCEPTIONS(
        contrib/libs/linux-headers  # DTCC-553
        contrib/libs/fmt            #
        contrib/libs/t1ha           # KIKIMR-16412
    )
ENDIF()


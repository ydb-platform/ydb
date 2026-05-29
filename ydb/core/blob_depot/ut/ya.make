UNITTEST_FOR(ydb/core/blob_depot)

    SIZE(MEDIUM)

    IF (NOT OS_WINDOWS)
        SRCS(
            s3_router_ut.cpp
        )

        PEERDIR(
            ydb/core/testlib/default
            ydb/library/actors/http
            ydb/library/aws_init
        )
    ENDIF()

    SRCS(
        closed_interval_set_ut.cpp
        given_id_range_ut.cpp
    )

END()

UNITTEST_FOR(ydb/core/blob_depot)

    SIZE(MEDIUM)

    SRCS(
        closed_interval_set_ut.cpp
        given_id_range_ut.cpp
    )

REQUIREMENTS(cpu:1)
END()

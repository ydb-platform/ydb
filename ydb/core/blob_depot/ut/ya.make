UNITTEST_FOR(ydb/core/blob_depot)

    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

    SRCS(
        closed_interval_set_ut.cpp
        given_id_range_ut.cpp
    )

END()

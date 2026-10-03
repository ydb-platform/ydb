GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
    ydb/core/nbs/cloud/storage/core/libs/user_stats/counter

    library/cpp/json
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/spack
    library/cpp/monlib/encode/text
    library/cpp/resource
)

RESOURCE(
    res/user_server_volume_instance_test.json user_server_volume_instance_test
    res/user_service_volume_instance_test.json user_service_volume_instance_test
    res/user_server_volume_instance_skip_zero_blocks_test.json user_server_volume_instance_skip_zero_blocks_test
)

SRCS(
    ../user_counter.cpp

    ../user_counter_ut.cpp
)

END()

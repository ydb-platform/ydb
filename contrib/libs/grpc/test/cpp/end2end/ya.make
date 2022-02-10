LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt) 

OWNER(dvshkurko) 
 
PEERDIR(
    contrib/libs/grpc/src/proto/grpc/health/v1
    contrib/libs/grpc/src/proto/grpc/testing
    contrib/libs/grpc/src/proto/grpc/testing/duplicate
    contrib/libs/grpc/test/cpp/util
    contrib/libs/grpc
    contrib/restricted/googletest/googlemock
    contrib/restricted/googletest/googletest
)

ADDINCL( 
    ${ARCADIA_BUILD_ROOT}/contrib/libs/grpc 
    contrib/libs/grpc 
) 

NO_COMPILER_WARNINGS()

SRCS(
    # async_end2end_test.cc
    # channelz_service_test.cc
    # client_callback_end2end_test.cc
    # client_crash_test.cc
    # client_crash_test_server.cc
    # client_interceptors_end2end_test.cc
    # client_lb_end2end_test.cc lb needs opencensus, not enabled.
    # end2end_test.cc
    # exception_test.cc
    # filter_end2end_test.cc
    # generic_end2end_test.cc
    # grpclb_end2end_test.cc lb needs opencensus, not enabled.
    # health_service_end2end_test.cc
    # hybrid_end2end_test.cc
    interceptors_util.cc
    # mock_test.cc
    # nonblocking_test.cc
    # proto_server_reflection_test.cc
    # raw_end2end_test.cc
    # server_builder_plugin_test.cc
    # server_crash_test.cc
    # server_crash_test_client.cc
    # server_early_return_test.cc
    # server_interceptors_end2end_test.cc
    # server_load_reporting_end2end_test.cc
    # shutdown_test.cc
    # streaming_throughput_test.cc
    test_health_check_service_impl.cc
    test_service_impl.cc
    # thread_stress_test.cc
    # time_change_test.cc
)

END()

RECURSE_FOR_TESTS(
    health
    server_interceptors
    # Needs new gtest
    # thread
)

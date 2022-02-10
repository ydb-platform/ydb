PY3TEST() 
 
LICENSE(Apache-2.0)
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/grpc/python
)

NO_LINT() 
 
PY_SRCS( 
    TOP_LEVEL 
    # tests/_sanity/__init__.py 
    # tests/testing/proto/__init__.py 
    # tests/testing/__init__.py 
    # tests/testing/_application_common.py 
    # tests/testing/_application_testing_common.py 
    # tests/testing/_client_application.py 
    # tests/testing/_client_test.py 
    # tests/testing/_server_application.py 
    # tests/testing/_server_test.py 
    # tests/testing/_time_test.py 
    tests/unit/__init__.py 
    tests/unit/_cython/__init__.py 
    tests/unit/_cython/_common.py 
    tests/unit/_cython/test_utilities.py 
    tests/unit/_exit_scenarios.py 
    tests/unit/_from_grpc_import_star.py 
    tests/unit/_rpc_test_helpers.py
    tests/unit/_server_shutdown_scenarios.py 
    tests/unit/_signal_client.py 
    tests/unit/_tcp_proxy.py 
    tests/unit/beta/__init__.py 
    tests/unit/beta/test_utilities.py 
    tests/unit/framework/__init__.py 
    tests/unit/framework/common/__init__.py 
    tests/unit/framework/common/test_constants.py 
    tests/unit/framework/common/test_control.py 
    tests/unit/framework/common/test_coverage.py 
    tests/unit/framework/foundation/__init__.py 
    tests/unit/resources.py 
    tests/unit/test_common.py 
    tests/unit/thread_pool.py 
    # protofiles 
    # tests/interop/__init__.py 
    # tests/interop/_intraop_test_case.py 
    # tests/interop/client.py 
    # tests/interop/methods.py 
    # tests/interop/resources.py 
    # tests/interop/server.py 
    # tests/interop/service.py 
    # protofiles 
    # tests/fork/__init__.py 
    # tests/fork/client.py 
    # tests/fork/methods.py 
    # protofiles 
    # tests/__init__.py 
    # tests/_loader.py 
    # tests/_result.py 
    # tests/_runner.py 
) 
 
TEST_SRCS( 
    # coverage 
    # tests/_sanity/_sanity_test.py 
    tests/unit/_api_test.py 
    tests/unit/_abort_test.py 
    # CRASH 
    # tests/unit/_auth_context_test.py 
    tests/unit/_auth_test.py 
    tests/unit/_channel_args_test.py 
    tests/unit/_channel_close_test.py 
    tests/unit/_channel_connectivity_test.py 
    tests/unit/_channel_ready_future_test.py 
    # FLAKY 
    # tests/unit/_compression_test.py 
    tests/unit/_contextvars_propagation_test.py
    tests/unit/_credentials_test.py 
    tests/unit/_cython/_cancel_many_calls_test.py 
    tests/unit/_cython/_channel_test.py 
    tests/unit/_cython/_fork_test.py 
    tests/unit/_cython/_no_messages_server_completion_queue_per_call_test.py 
    tests/unit/_cython/_no_messages_single_server_completion_queue_test.py 
    tests/unit/_cython/_read_some_but_not_all_responses_test.py 
    tests/unit/_cython/_server_test.py 
    tests/unit/_cython/cygrpc_test.py 
    tests/unit/_dns_resolver_test.py 
    tests/unit/_dynamic_stubs_test.py
    tests/unit/_empty_message_test.py 
    tests/unit/_error_message_encoding_test.py 
    tests/unit/_exit_test.py 
    tests/unit/_grpc_shutdown_test.py 
    tests/unit/_interceptor_test.py 
    tests/unit/_invalid_metadata_test.py 
    tests/unit/_invocation_defects_test.py 
    tests/unit/_local_credentials_test.py 
    tests/unit/_logging_test.py 
    tests/unit/_metadata_code_details_test.py 
    tests/unit/_metadata_flags_test.py 
    tests/unit/_metadata_test.py 
    tests/unit/_reconnect_test.py 
    tests/unit/_resource_exhausted_test.py 
    tests/unit/_rpc_part_1_test.py
    tests/unit/_rpc_part_2_test.py
    tests/unit/_server_shutdown_test.py 
    # tests.testing 
    # tests/unit/_server_ssl_cert_config_test.py 
    tests/unit/_server_test.py 
    tests/unit/_server_wait_for_termination_test.py 
    # CRASH 
    # tests/unit/_session_cache_test.py 
    tests/unit/_signal_handling_test.py 
    tests/unit/_version_test.py 
    tests/unit/beta/_beta_features_test.py 
    tests/unit/beta/_connectivity_channel_test.py 
    # oauth2client 
    # tests/unit/beta/_implementations_test.py 
    tests/unit/beta/_not_found_test.py 
    tests/unit/beta/_utilities_test.py 
    tests/unit/framework/foundation/_logging_pool_test.py 
    tests/unit/framework/foundation/stream_testing.py 
    # protofiles 
    # tests/interop/_insecure_intraop_test.py 
    # tests/interop/_secure_intraop_test.py 
    # tests/fork/_fork_interop_test.py 
) 
 
SIZE(MEDIUM) 
 
RESOURCE_FILES( 
    PREFIX contrib/libs/grpc/src/python/grpcio_tests/ 
    tests/unit/credentials/ca.pem 
    tests/unit/credentials/server1.key 
    tests/unit/credentials/server1.pem 
) 
 
REQUIREMENTS(network:full) 
 
END() 

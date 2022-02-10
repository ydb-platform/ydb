LIBRARY()

LICENSE(Apache-2.0) 
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(dvshkurko)

PEERDIR(
    contrib/libs/grpc
    contrib/restricted/abseil-cpp-tstring/y_absl/debugging/failure_signal_handler
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/grpc
    contrib/libs/grpc
)

NO_COMPILER_WARNINGS()

SRCS(
    # cmdline.cc
    # cmdline_test.cc
    # debugger_macros.cc
    # fuzzer_corpus_test.cc
    # fuzzer_one_entry_runner.sh*
    # fuzzer_util.cc
    # grpc_fuzzer.bzl
    # grpc_profiler.cc
    # histogram.cc
    # histogram_test.cc
    # lsan_suppressions.txt
    # memory_counters.cc
    # mock_endpoint.cc
    # one_corpus_entry_fuzzer.cc
    # parse_hexstring.cc
    # passthru_endpoint.cc
    port.cc
    # port_isolated_runtime_environment.cc
    port_server_client.cc
    # reconnect_server.cc
    # run_with_poller.sh*
    # slice_splitter.cc
    # subprocess_posix.cc
    # subprocess_windows.cc
    test_config.cc
    test_lb_policies.cc
    # test_tcp_server.cc
    # tracer_util.cc
    # trickle_endpoint.cc
)

END()

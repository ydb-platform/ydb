UNITTEST_FOR(library/cpp/messagebus)

TIMEOUT(1200)

SIZE(LARGE)

TAG(
    ya:not_autocheck
    ya:fat
)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/messagebus
    library/cpp/messagebus/test/helper
    library/cpp/messagebus/www
    library/cpp/resource
    library/cpp/deprecated/atomic
)

SRCS(
    messagebus_ut.cpp
    module_client_ut.cpp
    module_client_one_way_ut.cpp
    module_server_ut.cpp
    one_way_ut.cpp
    starter_ut.cpp
    sync_client_ut.cpp
    locator_uniq_ut.cpp
    www_ut.cpp
    ../../async_result_ut.cpp
    ../../cc_semaphore_ut.cpp
    ../../coreconn_ut.cpp
    ../../duration_histogram_ut.cpp
    ../../message_status_counter_ut.cpp
    ../../misc/weak_ptr_ut.cpp
    ../../latch_ut.cpp
    ../../lfqueue_batch_ut.cpp
    ../../local_flags_ut.cpp
    ../../memory_ut.cpp
    ../../moved_ut.cpp
    ../../netaddr_ut.cpp
    ../../network_ut.cpp
    ../../nondestroying_holder_ut.cpp
    ../../scheduler_actor_ut.cpp
    ../../socket_addr_ut.cpp
    ../../vector_swaps_ut.cpp
)

END()

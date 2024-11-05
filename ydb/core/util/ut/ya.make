UNITTEST_FOR(ydb/core/util)

FORK_SUBTESTS()
IF (WITH_VALGRIND)
    SPLIT_FACTOR(30)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/threading/future
)

SRCS(
    address_classifier_ut.cpp
    bits_ut.cpp
    btree_cow_ut.cpp
    btree_ut.cpp
    cache_cache_ut.cpp
    cache_ut.cpp
    circular_queue_ut.cpp
    concurrent_rw_hash_ut.cpp
    event_priority_queue_ut.cpp
    fast_tls_ut.cpp
    fragmented_buffer_ut.cpp
    hazard_ut.cpp
    hyperlog_counter_ut.cpp
    interval_set_ut.cpp
    intrusive_fixed_hash_set_ut.cpp
    intrusive_heap_ut.cpp
    intrusive_stack_ut.cpp
    lf_stack_ut.cpp
    log_priority_mute_checker_ut.cpp
    lz4_data_generator_ut.cpp
    operation_queue_priority_ut.cpp
    operation_queue_ut.cpp
    page_map_ut.cpp
    queue_inplace_ut.cpp
    queue_oneone_inplace_ut.cpp
    simple_cache_ut.cpp
    stlog_ut.cpp
    token_bucket_ut.cpp
    ui64id_ut.cpp
    ulid_ut.cpp
    wildcard_ut.cpp
)

END()

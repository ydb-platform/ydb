UNITTEST_FOR(ydb/library/actors/util)

IF (WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    cpu_load_log_ut.cpp
    memory_tracker_ut.cpp
    thread_load_log_ut.cpp
    rope_ut.cpp
    rc_buf_ut.cpp
    shared_data_ut.cpp
    shared_data_rope_backend_ut.cpp
    shared_data_native_rope_backend_ut.cpp
    unordered_cache_ut.cpp
)

END()

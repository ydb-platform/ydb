LIBRARY()

LICENSE(BSL-1.0) 
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

ADDINCL(
    ${BOOST_ROOT}/libs/log/src
)

CFLAGS(
    -DBOOST_LOG_BUILDING_THE_LIB=1
    -DBOOST_LOG_SETUP_BUILDING_THE_LIB=1
)

IF (OS_WINDOWS)
    CFLAGS(
        -D_CRT_SECURE_NO_DEPRECATE
        -D_SCL_SECURE_NO_DEPRECATE
        -D_SCL_SECURE_NO_WARNINGS
    )
ELSE()
    IF (OS_LINUX AND OS_SDK != "ubuntu-10")
        CFLAGS(
            -DBOOST_LOG_HAS_PTHREAD_MUTEX_ROBUST
        )
    ENDIF()
    CFLAGS(
        -DBOOST_LOG_USE_NATIVE_SYSLOG
        -DBOOST_LOG_WITHOUT_DEBUG_OUTPUT
        -DBOOST_LOG_WITHOUT_EVENT_LOG
        -D_XOPEN_SOURCE=600
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        src/windows/debug_output_backend.cpp
        src/windows/event_log_backend.cpp
        src/windows/ipc_reliable_message_queue.cpp
        src/windows/ipc_sync_wrappers.cpp
        src/windows/light_rw_mutex.cpp
        src/windows/mapped_shared_memory.cpp
        src/windows/object_name.cpp
    )
ELSE()
    SRCS(
        src/posix/ipc_reliable_message_queue.cpp
        src/posix/object_name.cpp
    )
ENDIF()

SRCS(
    src/attribute_name.cpp
    src/attribute_set.cpp
    src/attribute_value_set.cpp
    src/code_conversion.cpp
    src/core.cpp
    src/date_time_format_parser.cpp
    src/default_attribute_names.cpp
    src/default_sink.cpp
    src/dump.cpp
    src/event.cpp
    src/exceptions.cpp
    src/format_parser.cpp
    src/global_logger_storage.cpp
    src/named_scope.cpp
    src/named_scope_format_parser.cpp
    src/once_block.cpp
    src/permissions.cpp
    src/process_id.cpp
    src/process_name.cpp
    src/record_ostream.cpp
    src/setup/default_filter_factory.cpp
    src/setup/default_formatter_factory.cpp
    src/setup/filter_parser.cpp
    src/setup/formatter_parser.cpp
    src/setup/init_from_settings.cpp
    src/setup/init_from_stream.cpp
    src/setup/matches_relation_factory.cpp
    src/setup/parser_utils.cpp
    src/setup/settings_parser.cpp
    src/severity_level.cpp
    src/spirit_encoding.cpp
    src/syslog_backend.cpp
    src/text_file_backend.cpp
    src/text_multifile_backend.cpp
    src/text_ostream_backend.cpp
    src/thread_id.cpp
    src/thread_specific.cpp
    src/threadsafe_queue.cpp
    src/timer.cpp
    src/timestamp.cpp
    src/trivial.cpp
    src/unhandled_exception_count.cpp
)

END()

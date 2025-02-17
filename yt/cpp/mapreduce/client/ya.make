LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    abortable_registry.cpp
    batch_request_impl.cpp
    client_reader.cpp
    client_writer.cpp
    client.cpp
    file_reader.cpp
    file_writer.cpp
    format_hints.cpp
    init.cpp
    lock.cpp
    operation_helpers.cpp
    operation_preparer.cpp
    operation_tracker.cpp
    operation.cpp
    prepare_operation.cpp
    py_helpers.cpp
    retry_heavy_write_request.cpp
    retryful_writer.cpp
    retryful_writer_v2.cpp
    retryless_writer.cpp
    skiff.cpp
    structured_table_formats.cpp
    transaction.cpp
    transaction_pinger.cpp
    yt_poller.cpp
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/sighandler
    library/cpp/threading/blocking_queue
    library/cpp/threading/future
    library/cpp/type_info
    library/cpp/yson
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/http
    yt/cpp/mapreduce/http_client
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/io
)

PEERDIR(
    yt/yt/core
    yt/yt/core/http
)

IF (BUILD_TYPE == "PROFILE")
    PEERDIR(
        yt/yt/library/ytprof
    )

    IF (OPENSOURCE)
        SRCS(
            dummy_job_profiler.cpp
        )
    ELSE()
        SRCS(
            job_profiler.cpp
        )
    ENDIF()
ELSE()
    SRCS(
        dummy_job_profiler.cpp
    )
ENDIF()

GENERATE_ENUM_SERIALIZATION(structured_table_formats.h)

END()

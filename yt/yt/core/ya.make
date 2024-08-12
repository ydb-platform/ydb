LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../ya_check_dependencies.inc)

IF (ARCH_X86_64)
    # pclmul is required for fast crc computation
    CFLAGS(-mpclmul)
ENDIF()

NO_LTO()

SRCS(
    actions/cancelable_context.cpp
    actions/current_invoker.cpp
    actions/future.cpp
    actions/invoker_detail.cpp
    actions/invoker_pool.cpp
    actions/invoker_util.cpp

    bus/public.cpp

    bus/tcp/connection.cpp
    bus/tcp/dispatcher.cpp
    bus/tcp/dispatcher_impl.cpp
    bus/tcp/config.cpp
    bus/tcp/packet.cpp
    bus/tcp/client.cpp
    bus/tcp/server.cpp
    bus/tcp/ssl_context.cpp
    bus/tcp/ssl_helpers.cpp

    compression/brotli.cpp
    compression/bzip2.cpp
    compression/codec.cpp
    compression/dictionary_codec.cpp
    compression/stream.cpp
    compression/lz.cpp
    compression/lzma.cpp
    compression/public.cpp
    compression/snappy.cpp
    compression/zlib.cpp
    compression/zstd.cpp

    concurrency/action_queue.cpp
    concurrency/async_barrier.cpp
    concurrency/async_looper.cpp
    concurrency/async_rw_lock.cpp
    concurrency/async_semaphore.cpp
    concurrency/async_stream_pipe.cpp
    concurrency/async_stream.cpp
    concurrency/config.cpp
    concurrency/coroutine.cpp
    concurrency/delayed_executor.cpp
    concurrency/execution_stack.cpp
    concurrency/fair_share_action_queue.cpp
    concurrency/fair_share_invoker_pool.cpp
    concurrency/fair_share_invoker_queue.cpp
    concurrency/fair_share_queue_scheduler_thread.cpp
    concurrency/fair_share_thread_pool.cpp
    concurrency/fair_throttler.cpp
    concurrency/fiber_scheduler_thread.cpp
    concurrency/fiber.cpp
    concurrency/fls.cpp
    concurrency/invoker_alarm.cpp
    concurrency/invoker_queue.cpp
    concurrency/lease_manager.cpp
    concurrency/new_fair_share_thread_pool.cpp
    concurrency/nonblocking_batcher.cpp
    concurrency/notify_manager.cpp
    concurrency/periodic_executor.cpp
    concurrency/periodic_yielder.cpp
    concurrency/pollable_detail.cpp
    concurrency/profiling_helpers.cpp
    concurrency/propagating_storage.cpp
    concurrency/quantized_executor.cpp
    concurrency/scheduler_thread.cpp
    concurrency/single_queue_scheduler_thread.cpp
    concurrency/suspendable_action_queue.cpp
    concurrency/system_invokers.cpp
    concurrency/thread_affinity.cpp
    concurrency/thread_pool_detail.cpp
    concurrency/thread_pool_poller.cpp
    concurrency/thread_pool.cpp
    concurrency/throughput_throttler.cpp
    concurrency/two_level_fair_share_thread_pool.cpp
    concurrency/retrying_periodic_executor.cpp
    concurrency/scheduled_executor.cpp

    crypto/config.cpp
    crypto/crypto.cpp
    crypto/tls.cpp

    logging/compression.cpp
    logging/config.cpp
    logging/formatter.cpp
    logging/fluent_log.cpp
    GLOBAL logging/log.cpp
    GLOBAL logging/log_manager.cpp
    logging/logger_owner.cpp
    logging/serializable_logger.cpp
    logging/stream_output.cpp
    logging/log_writer_detail.cpp
    logging/file_log_writer.cpp
    logging/stream_log_writer.cpp
    logging/random_access_gzip.cpp
    logging/zstd_compression.cpp

    misc/arithmetic_formula.cpp
    GLOBAL misc/assert.cpp
    misc/backoff_strategy.cpp
    misc/bitmap.cpp
    misc/bit_packed_unsigned_vector.cpp
    misc/bit_packing.cpp
    misc/blob_output.cpp
    misc/bloom_filter.cpp
    misc/checksum.cpp
    misc/config.cpp
    misc/coro_pipe.cpp
    misc/crash_handler.cpp
    misc/digest.cpp
    misc/error.cpp
    misc/error_code.cpp
    misc/ema_counter.cpp
    misc/fs.cpp
    # NB: it is necessary to prevent linker optimization of
    # REGISTER_INTERMEDIATE_PROTO_INTEROP_REPRESENTATION macros for TGuid.
    GLOBAL misc/guid.cpp
    misc/hazard_ptr.cpp
    misc/hedging_manager.cpp
    misc/histogram.cpp
    misc/adjusted_exponential_moving_average.cpp
    misc/id_generator.cpp
    misc/linear_probe.cpp
    misc/memory_usage_tracker.cpp
    misc/relaxed_mpsc_queue.cpp
    misc/parser_helpers.cpp
    misc/pattern_formatter.cpp
    misc/phoenix.cpp
    misc/pool_allocator.cpp
    misc/proc.cpp
    misc/process_exit_profiler.cpp
    misc/protobuf_helpers.cpp
    misc/public.cpp
    misc/random.cpp
    misc/ref_counted_tracker.cpp
    misc/ref_counted_tracker_statistics_producer.cpp
    misc/ref_counted_tracker_profiler.cpp
    GLOBAL misc/ref_tracked.cpp
    misc/serialize.cpp
    misc/shutdown.cpp
    misc/signal_registry.cpp
    misc/slab_allocator.cpp
    misc/statistic_path.cpp
    misc/statistics.cpp
    misc/string_helpers.cpp
    misc/cache_config.cpp
    misc/utf8_decoder.cpp
    misc/zerocopy_output_writer.cpp

    net/address.cpp
    net/connection.cpp
    net/config.cpp
    net/dialer.cpp
    net/helpers.cpp
    net/listener.cpp
    net/local_address.cpp
    net/public.cpp
    net/socket.cpp

    dns/ares_dns_resolver.cpp
    dns/config.cpp
    dns/dns_resolver.cpp

    profiling/timing.cpp

    phoenix/context.cpp
    phoenix/descriptors.cpp
    phoenix/load.cpp
    phoenix/schemas.cpp
    phoenix/type_def.cpp
    phoenix/type_registry.cpp

    rpc/authentication_identity.cpp
    rpc/authenticator.cpp
    rpc/balancing_channel.cpp
    rpc/caching_channel_factory.cpp
    rpc/channel_detail.cpp
    rpc/client.cpp
    rpc/config.cpp
    rpc/dispatcher.cpp
    rpc/dynamic_channel_pool.cpp
    rpc/hedging_channel.cpp
    rpc/helpers.cpp
    rpc/local_channel.cpp
    rpc/local_server.cpp
    rpc/message.cpp
    rpc/message_format.cpp
    rpc/null_channel.cpp
    rpc/peer_discovery.cpp
    rpc/per_user_request_queue_provider.cpp
    rpc/protocol_version.cpp
    rpc/public.cpp
    rpc/request_queue_provider.cpp
    rpc/response_keeper.cpp
    rpc/retrying_channel.cpp
    rpc/roaming_channel.cpp
    rpc/serialized_channel.cpp
    rpc/server_detail.cpp
    rpc/service.cpp
    rpc/service_detail.cpp
    rpc/static_channel_factory.cpp
    rpc/stream.cpp
    rpc/throttling_channel.cpp
    rpc/viable_peer_registry.cpp

    rpc/bus/server.cpp
    rpc/bus/channel.cpp

    service_discovery/service_discovery.cpp

    threading/spin_wait_slow_path_logger.cpp
    threading/thread.cpp

    GLOBAL tracing/allocation_hooks.cpp
    tracing/allocation_tags.cpp
    tracing/config.cpp
    tracing/public.cpp
    GLOBAL tracing/trace_context.cpp

    utilex/random.cpp

    ypath/stack.cpp
    ypath/token.cpp
    ypath/tokenizer.cpp
    ypath/helpers.cpp

    yson/async_consumer.cpp
    yson/async_writer.cpp
    yson/attribute_consumer.cpp
    yson/config.cpp
    yson/consumer.cpp
    yson/forwarding_consumer.cpp
    yson/lexer.cpp
    yson/null_consumer.cpp
    yson/parser.cpp
    yson/producer.cpp
    yson/protobuf_interop.cpp
    yson/protobuf_interop_options.cpp
    yson/protobuf_interop_unknown_fields.cpp
    yson/pull_parser.cpp
    yson/pull_parser_deserialize.cpp
    yson/stream.cpp
    yson/string.cpp
    yson/string_builder_stream.cpp
    yson/string_filter.cpp
    yson/syntax_checker.cpp
    yson/token.cpp
    yson/token_writer.cpp
    yson/tokenizer.cpp
    yson/writer.cpp
    yson/string_merger.cpp
    yson/ypath_designated_consumer.cpp
    yson/ypath_filtering_consumer.cpp
    yson/depth_limiting_yson_consumer.cpp
    yson/list_verb_lazy_yson_consumer.cpp
    yson/attributes_stripper.cpp

    ytree/attribute_consumer.cpp
    ytree/helpers.cpp
    ytree/attributes.cpp
    ytree/attribute_filter.cpp
    ytree/convert.cpp
    ytree/ephemeral_attribute_owner.cpp
    ytree/ephemeral_node_factory.cpp
    ytree/exception_helpers.cpp
    ytree/interned_attributes.cpp
    ytree/node.cpp
    ytree/node_detail.cpp
    ytree/permission.cpp
    ytree/request_complexity_limiter.cpp
    ytree/request_complexity_limits.cpp
    ytree/serialize.cpp
    ytree/static_service_dispatcher.cpp
    ytree/system_attribute_provider.cpp
    ytree/tree_builder.cpp
    ytree/tree_visitor.cpp
    ytree/virtual.cpp
    ytree/service_combiner.cpp
    ytree/ypath_client.cpp
    ytree/ypath_detail.cpp
    ytree/ypath_resolver.cpp
    ytree/ypath_service.cpp
    ytree/yson_struct.cpp
    ytree/yson_struct_detail.cpp

    json/config.cpp
    json/json_callbacks.cpp
    json/helpers.cpp
    json/json_parser.cpp
    json/json_writer.cpp

    ytalloc/bindings.cpp
    ytalloc/config.cpp
    ytalloc/statistics_producer.cpp
)

IF (OS_LINUX OR OS_FREEBSD)
    EXTRALIBS(-lutil)
ENDIF()

PEERDIR(
    contrib/libs/snappy
    contrib/libs/zlib
    contrib/libs/zstd
    contrib/libs/lzmasdk
    contrib/libs/libbz2
    contrib/libs/c-ares
    contrib/libs/farmhash
    contrib/libs/yajl
    contrib/libs/lz4
    contrib/libs/openssl

    library/cpp/openssl/init
    library/cpp/openssl/io
    library/cpp/threading/thread_local
    library/cpp/streams/brotli
    library/cpp/yt/assert
    library/cpp/yt/containers
    library/cpp/yt/logging
    library/cpp/yt/logging/plain_text_formatter
    library/cpp/yt/misc
    library/cpp/yt/memory
    library/cpp/yt/string
    library/cpp/yt/yson
    library/cpp/yt/yson_string
    library/cpp/ytalloc/api

    yt/yt/build

    yt/yt/core/misc/isa_crc64

    yt/yt_proto/yt/core

    library/cpp/yt/assert
    library/cpp/yt/backtrace
    library/cpp/yt/coding
    library/cpp/yt/malloc
    library/cpp/yt/small_containers
    library/cpp/yt/system
    library/cpp/yt/threading

    yt/yt/library/syncmap
    yt/yt/library/undumpable
    yt/yt/library/ytprof/api

    # TODO(prime@): remove this, once yt/core is split into separate libraries.
    yt/yt/library/profiling
    yt/yt/library/profiling/resource_tracker
    yt/yt/library/tracing
)

IF (OS_WINDOWS)
    PEERDIR(
        library/cpp/yt/backtrace/cursors/dummy
    )
ELSE()
    PEERDIR(
        library/cpp/yt/backtrace/cursors/libunwind
    )
ENDIF()

END()

RECURSE(
    http
    test_framework
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmarks
        bus/benchmarks
        yson/benchmark
    )
ENDIF()

RECURSE_FOR_TESTS(
    actions/unittests
    concurrency/unittests
    http/unittests
    misc/unittests
    net/unittests
    tracing/unittests
    yson/unittests
    http/mock
    net/mock
)

IF (NOT OS_WINDOWS)
    RECURSE(
        misc/isa_crc64
        service_discovery/yp
    )

    RECURSE_FOR_TESTS(
        bus/unittests
        compression/unittests
        crypto/unittests
        json/unittests
        logging/unittests
        phoenix/unittests
        profiling/unittests
        rpc/unittests
        ypath/unittests
        ytree/unittests
    )
ENDIF()

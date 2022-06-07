
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


find_package(OpenSSL REQUIRED)

add_library(libs-grpc-grpc++)
target_compile_options(libs-grpc-grpc++ PRIVATE
  -DGRPC_POSIX_FORK_ALLOW_PTHREAD_ATFORK=1
)
target_include_directories(libs-grpc-grpc++ PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/include
)
target_include_directories(libs-grpc-grpc++ PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/upb-generated
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb
)
target_link_libraries(libs-grpc-grpc++ PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-grpc-grpc
  src-core-lib
  grpc-third_party-address_sorting
  grpc-third_party-upb
  OpenSSL::OpenSSL
  ycontainer-internal-raw_hash_set
  yabseil-cpp-tstring-y_absl-hash
  yabseil-cpp-tstring-y_absl-status
  yabsl-status-statusor
  yabseil-cpp-tstring-y_absl-strings
  yabseil-cpp-tstring-y_absl-synchronization
  yabseil-cpp-tstring-y_absl-time
)
target_sources(libs-grpc-grpc++ PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/binder_connector.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/channel_create.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/channel_create_impl.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/connection_id_generator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/endpoint_binder_pool.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/jni_utils.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/client/security_policy_setting.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/security_policy/binder_security_policy.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/server/binder_server.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/server/binder_server_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/transport/binder_transport.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/utils/ndk_binder.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/utils/transport_stream_receiver_impl.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/wire_format/binder_android.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/wire_format/binder_constants.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/wire_format/transaction.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/wire_format/wire_reader_impl.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/transport/binder/wire_format/wire_writer.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/channel_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/client_callback.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/client_context.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/client_interceptor.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/create_channel.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/create_channel_internal.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/create_channel_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/credentials_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/insecure_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/secure_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/client/xds_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/codegen/codegen_init.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/alarm.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/auth_property_iterator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/channel_arguments.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/channel_filter.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/completion_queue_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/core_codegen.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/resource_quota_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/rpc_method.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/secure_auth_context.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/secure_channel_arguments.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/secure_create_auth_context.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/tls_certificate_provider.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/tls_certificate_verifier.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/tls_credentials_options.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/validate_service_config.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/common/version_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/async_generic_service.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/channel_argument_option.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/create_default_thread_pool.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/dynamic_thread_pool.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/external_connection_acceptor_impl.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/health/default_health_check_service.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/health/health_check_service.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/health/health_check_service_server_builder_option.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/insecure_server_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/load_reporter/get_cpu_stats_linux.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/load_reporter/get_cpu_stats_macos.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/load_reporter/get_cpu_stats_unsupported.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/load_reporter/get_cpu_stats_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/load_reporter/load_data_store.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/load_reporter/util.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/secure_server_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/server_builder.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/server_callback.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/server_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/server_context.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/server_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/server_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/server/xds_server_credentials.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/thread_manager/thread_manager.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/util/byte_buffer_cc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/util/status.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/util/string_ref.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/cpp/util/time_cc.cc
)

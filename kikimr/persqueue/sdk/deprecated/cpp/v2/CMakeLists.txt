
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(deprecated-cpp-v2)
target_link_libraries(deprecated-cpp-v2 PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-protobuf
  cpp-client-iam
  cpp-client-ydb_persqueue
  api-grpc-yndx
  api-protos-yndx
  yndx-persqueue-read_batch_converter
  library-cpp-blockcodecs
  cpp-containers-disjoint_interval_tree
  cpp-containers-intrusive_rb_tree
  cpp-grpc-common
  cpp-http-simple
  library-cpp-json
  library-cpp-logger
  cpp-streams-lzop
  cpp-streams-zstd
  cpp-string_utils-quote
  cpp-threading-future
  cpp-tvmauth-client
  library-persqueue-topic_parser_public
  cpp-client-resources
)
target_sources(deprecated-cpp-v2 PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/channel.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/compressing_producer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/decompressing_consumer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/iconsumer_p.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/interface_common.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/iprocessor_p.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/iproducer_p.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/multicluster_consumer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/multicluster_producer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/persqueue.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/processor.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/producer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/logger.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/types.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/queue_pool.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/retrying_consumer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/retrying_producer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/scheduler.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/consumer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/credentials_provider.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/validate_grpc_metadata.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/compat_producer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/impl/ydb_sdk_consumer.cpp
)

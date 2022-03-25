
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(libs-config-protos)
target_link_libraries(libs-config-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-folder_service-proto
  providers-common-proto
  providers-s3-proto
  contrib-libs-protobuf
)
target_proto_messages(libs-config-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/audit.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/checkpoint_coordinator.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/common.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/control_plane_proxy.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/control_plane_storage.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/db_pool.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/gateways.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/issue_id.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/nodes_manager.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/pending_fetcher.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/pinger.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/private_api.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/private_proxy.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/read_actors_factory.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/resource_manager.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/storage.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/test_connection.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/token_accessor.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/protos/yq_config.proto
)
target_sources(libs-config-protos PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/audit.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/checkpoint_coordinator.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/common.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/control_plane_proxy.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/control_plane_storage.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/db_pool.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/gateways.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/issue_id.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/nodes_manager.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/pending_fetcher.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/pinger.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/private_api.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/private_proxy.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/read_actors_factory.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/resource_manager.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/storage.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/test_connection.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/token_accessor.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/core/yq/libs/config/protos/yq_config.pb.cc
)

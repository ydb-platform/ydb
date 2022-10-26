
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(priv-ydb-v1)
set_property(TARGET priv-ydb-v1 PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET priv-ydb-v1 PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/private-api
)
target_include_directories(priv-ydb-v1 PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_link_libraries(priv-ydb-v1 PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  yandex-cloud-api
  cloud-api-tools
  yandex-cloud-priv
  cloud-priv-access
  cloud-priv-operation
  cloud-priv-quota
  contrib-libs-protobuf
)
target_proto_messages(priv-ydb-v1 PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/backup.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/backup_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/database.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/database_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/location.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/location_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/operation_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/quota_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/resource_preset.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/resource_preset_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/storage_type.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/storage_type_service.proto
)
target_proto_addincls(priv-ydb-v1
  ./cloud/bitbucket/private-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/common-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(priv-ydb-v1
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_proto_plugin(priv-ydb-v1
  grpc_cpp
  grpc_cpp
)

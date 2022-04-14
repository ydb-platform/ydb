
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yandex-cloud-operation)
set_property(TARGET yandex-cloud-operation PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET yandex-cloud-operation PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/public-api
)
target_include_directories(yandex-cloud-operation PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_include_directories(yandex-cloud-operation PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_link_libraries(yandex-cloud-operation PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  cloud-api-tools
  public-api-yandex-cloud
  contrib-libs-protobuf
)
target_proto_messages(yandex-cloud-operation PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/operation/operation.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/operation/operation_service.proto
)
target_proto_addincls(yandex-cloud-operation
  ./cloud/bitbucket/public-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/common-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(yandex-cloud-operation
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_proto_plugin(yandex-cloud-operation
  grpc_cpp
  grpc_cpp
)


# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(tools)

add_library(yandex-cloud-api)
set_property(TARGET yandex-cloud-api PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET yandex-cloud-api PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/common-api
)
target_include_directories(yandex-cloud-api PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/common-api
)
target_link_libraries(yandex-cloud-api PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  cloud-api-tools
  contrib-libs-protobuf
)
target_proto_messages(yandex-cloud-api PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/common-api/yandex/cloud/api/operation.proto
)
target_proto_addincls(yandex-cloud-api
  ./cloud/bitbucket/common-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/common-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/common-api
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/common-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(yandex-cloud-api
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/common-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/common-api
)
target_proto_plugin(yandex-cloud-api
  grpc_cpp
  grpc_cpp
)

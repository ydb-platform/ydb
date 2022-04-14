
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yandex-cloud-access)
set_property(TARGET yandex-cloud-access PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET yandex-cloud-access PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/public-api
)
target_include_directories(yandex-cloud-access PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_include_directories(yandex-cloud-access PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_link_libraries(yandex-cloud-access PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  public-api-yandex-cloud
  contrib-libs-protobuf
)
target_proto_messages(yandex-cloud-access PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/access/access.proto
)
target_proto_addincls(yandex-cloud-access
  ./cloud/bitbucket/public-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(yandex-cloud-access
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_proto_plugin(yandex-cloud-access
  grpc_cpp
  grpc_cpp
)

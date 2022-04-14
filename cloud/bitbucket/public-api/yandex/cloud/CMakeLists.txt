
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(public-api-yandex-cloud)
set_property(TARGET public-api-yandex-cloud PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET public-api-yandex-cloud PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/public-api
)
target_include_directories(public-api-yandex-cloud PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_include_directories(public-api-yandex-cloud PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_link_libraries(public-api-yandex-cloud PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  contrib-libs-protobuf
)
target_proto_messages(public-api-yandex-cloud PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/imports.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/validation.proto
)
target_proto_addincls(public-api-yandex-cloud
  ./cloud/bitbucket/public-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(public-api-yandex-cloud
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_proto_plugin(public-api-yandex-cloud
  grpc_cpp
  grpc_cpp
)


# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cloud-priv-access)
set_property(TARGET cloud-priv-access PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET cloud-priv-access PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/private-api
)
target_include_directories(cloud-priv-access PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_include_directories(cloud-priv-access PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_link_libraries(cloud-priv-access PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  yandex-cloud-priv
  contrib-libs-protobuf
)
target_proto_messages(cloud-priv-access PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/access/access.proto
)
target_proto_addincls(cloud-priv-access
  ./cloud/bitbucket/private-api
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(cloud-priv-access
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_proto_plugin(cloud-priv-access
  grpc_cpp
  grpc_cpp
)

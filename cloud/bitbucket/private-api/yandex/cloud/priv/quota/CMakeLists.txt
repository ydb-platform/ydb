
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cloud-priv-quota)
set_property(TARGET cloud-priv-quota PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET cloud-priv-quota PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/private-api
)
target_include_directories(cloud-priv-quota PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_link_libraries(cloud-priv-quota PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  yandex-cloud-priv
  contrib-libs-protobuf
)
target_proto_messages(cloud-priv-quota PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/quota/quota.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/quota/quota_limit.proto
)
target_proto_addincls(cloud-priv-quota
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
target_proto_outs(cloud-priv-quota
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_proto_plugin(cloud-priv-quota
  grpc_cpp
  grpc_cpp
)

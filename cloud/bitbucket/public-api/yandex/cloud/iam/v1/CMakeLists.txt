
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cloud-iam-v1)
set_property(TARGET cloud-iam-v1 PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
set_property(TARGET cloud-iam-v1 PROPERTY
  PROTO_NAMESPACE cloud/bitbucket/public-api
)
target_include_directories(cloud-iam-v1 PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_include_directories(cloud-iam-v1 PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_link_libraries(cloud-iam-v1 PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  yandex-cloud-api
  cloud-api-tools
  public-api-yandex-cloud
  yandex-cloud-access
  yandex-cloud-operation
  contrib-libs-protobuf
)
target_proto_messages(cloud-iam-v1 PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/api_key.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/api_key_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/iam_token_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/key.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/key_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/role.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/role_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/service_account.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/service_account_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/user_account.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/user_account_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/public-api/yandex/cloud/iam/v1/yandex_passport_user_account_service.proto
)
target_proto_addincls(cloud-iam-v1
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
target_proto_outs(cloud-iam-v1
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/public-api
)
target_proto_plugin(cloud-iam-v1
  grpc_cpp
  grpc_cpp
)

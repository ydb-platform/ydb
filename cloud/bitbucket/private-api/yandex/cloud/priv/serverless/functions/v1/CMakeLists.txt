
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(serverless-functions-v1)
set_property(TARGET serverless-functions-v1 PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_include_directories(serverless-functions-v1 PUBLIC
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_include_directories(serverless-functions-v1 PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_link_libraries(serverless-functions-v1 PUBLIC
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
target_proto_messages(serverless-functions-v1 PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/operation_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/quota_service.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/session.proto
  ${CMAKE_SOURCE_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/session_service.proto
)
target_sources(serverless-functions-v1 PRIVATE
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function.pb.cc
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function_service.pb.cc
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/operation_service.pb.cc
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/quota_service.pb.cc
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/session.pb.cc
  ${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/session_service.pb.cc
)
target_proto_addincls(serverless-functions-v1
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
target_proto_outs(serverless-functions-v1
  --cpp_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/cloud/bitbucket/private-api
)
target_proto_plugin(serverless-functions-v1
  grpc_cpp
  grpc_cpp
)

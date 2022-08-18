
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(iam-proto-v1)
set_property(TARGET iam-proto-v1 PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(iam-proto-v1 PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-googleapis-common-protos
  contrib-libs-protobuf
)
target_proto_messages(iam-proto-v1 PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/iam/proto/v1/iam_token_service.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/iam/proto/v1/validation.proto
)
target_proto_addincls(iam-proto-v1
  ./
  ${CMAKE_SOURCE_DIR}/
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(iam-proto-v1
  --cpp_out=${CMAKE_BINARY_DIR}/
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/
)
target_proto_plugin(iam-proto-v1
  grpc_cpp
  grpc_cpp
)

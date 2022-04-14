
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(api-grpc-yndx)
set_property(TARGET api-grpc-yndx PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(api-grpc-yndx PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  api-protos-yndx
  api-protos
  contrib-libs-protobuf
)
target_proto_messages(api-grpc-yndx PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/api/grpc/persqueue.proto
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/api/grpc/ydb_yndx_keyvalue_v1.proto
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/api/grpc/ydb_yndx_rate_limiter_v1.proto
)
target_proto_addincls(api-grpc-yndx
  ./
  ${CMAKE_SOURCE_DIR}/
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(api-grpc-yndx
  --cpp_out=${CMAKE_BINARY_DIR}/
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/
)
target_proto_plugin(api-grpc-yndx
  grpc_cpp
  grpc_cpp
)

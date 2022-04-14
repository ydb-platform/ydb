
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(api-protos-yndx)
target_link_libraries(api-protos-yndx PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
  tools-enum_parser-enum_serialization_runtime
  contrib-libs-protobuf
)
target_proto_messages(api-protos-yndx PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/api/protos/persqueue.proto
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/api/protos/ydb_yndx_keyvalue.proto
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/api/protos/ydb_yndx_rate_limiter.proto
)
generate_enum_serilization(api-protos-yndx
  ${CMAKE_BINARY_DIR}/kikimr/yndx/api/protos/persqueue.pb.h
  INCLUDE_HEADERS
  kikimr/yndx/api/protos/persqueue.pb.h
)
target_proto_addincls(api-protos-yndx
  ./
  ${CMAKE_SOURCE_DIR}/
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(api-protos-yndx
  --cpp_out=${CMAKE_BINARY_DIR}/
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/
)

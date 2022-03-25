
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(library-mkql_proto-protos)
set_property(TARGET library-mkql_proto-protos PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(library-mkql_proto-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-protobuf
)
target_proto_messages(library-mkql_proto-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/mkql_proto/protos/minikql.proto
)
target_sources(library-mkql_proto-protos PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/mkql_proto/protos/minikql.pb.cc
)
target_proto_plugin(library-mkql_proto-protos
  grpc_cpp
  grpc_cpp
)

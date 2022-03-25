
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cpp-lwtrace-protos)
target_link_libraries(cpp-lwtrace-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(cpp-lwtrace-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/protos/lwtrace.proto
)
target_sources(cpp-lwtrace-protos PRIVATE
  ${CMAKE_BINARY_DIR}/library/cpp/lwtrace/protos/lwtrace.pb.cc
)

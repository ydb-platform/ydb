
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cpp-actors-protos)
target_link_libraries(cpp-actors-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(cpp-actors-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/actors.proto
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/interconnect.proto
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/services_common.proto
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/unittests.proto
)
target_sources(cpp-actors-protos PRIVATE
  ${CMAKE_BINARY_DIR}/library/cpp/actors/protos/actors.pb.cc
  ${CMAKE_BINARY_DIR}/library/cpp/actors/protos/interconnect.pb.cc
  ${CMAKE_BINARY_DIR}/library/cpp/actors/protos/services_common.pb.cc
  ${CMAKE_BINARY_DIR}/library/cpp/actors/protos/unittests.pb.cc
)

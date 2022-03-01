
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(histogram-adaptive-protos)
target_link_libraries(histogram-adaptive-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(histogram-adaptive-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/adaptive/protos/histo.proto
)

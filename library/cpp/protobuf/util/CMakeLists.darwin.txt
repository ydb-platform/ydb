
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(proto)

add_library(cpp-protobuf-util)
target_link_libraries(cpp-protobuf-util PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  library-cpp-binsaver
  protobuf-util-proto
  cpp-string_utils-base64
)
target_sources(cpp-protobuf-util PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/is_equal.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/merge.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/path.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/pb_io.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/simple_reflection.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/walk.cpp
)

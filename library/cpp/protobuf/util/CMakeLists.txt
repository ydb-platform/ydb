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

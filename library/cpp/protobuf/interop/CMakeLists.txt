add_library(cpp-protobuf-interop)
target_link_libraries(cpp-protobuf-interop PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_sources(cpp-protobuf-interop PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/interop/cast.cpp
)

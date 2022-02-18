add_library(library-cpp-binsaver)
target_link_libraries(library-cpp-binsaver PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-2d_array
)
target_sources(library-cpp-binsaver PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/binsaver/bin_saver.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/binsaver/blob_io.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/binsaver/buffered_io.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/binsaver/mem_io.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/binsaver/util_stream_io.cpp
)

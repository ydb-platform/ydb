add_library(library-cpp-diff)
target_link_libraries(library-cpp-diff PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lcs
  cpp-containers-stack_array
)
target_sources(library-cpp-diff PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/diff/diff.cpp
)

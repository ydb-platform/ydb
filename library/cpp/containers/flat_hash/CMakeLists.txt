add_library(cpp-containers-flat_hash)
target_link_libraries(cpp-containers-flat_hash PUBLIC
  contrib-libs-cxxsupp
  yutil
  containers-flat_hash-lib
)
target_sources(cpp-containers-flat_hash PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/flat_hash.cpp
)

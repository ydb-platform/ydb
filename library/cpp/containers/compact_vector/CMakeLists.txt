add_library(cpp-containers-compact_vector)
target_link_libraries(cpp-containers-compact_vector PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-compact_vector PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/compact_vector/compact_vector.cpp
)

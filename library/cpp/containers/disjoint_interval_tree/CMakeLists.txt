add_library(cpp-containers-disjoint_interval_tree)
target_link_libraries(cpp-containers-disjoint_interval_tree PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-disjoint_interval_tree PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.cpp
)

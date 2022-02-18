add_library(cpp-containers-sorted_vector)
target_link_libraries(cpp-containers-sorted_vector PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-sorted_vector PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/sorted_vector/sorted_vector.cpp
)

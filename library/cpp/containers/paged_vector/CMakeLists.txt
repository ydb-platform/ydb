add_library(cpp-containers-paged_vector)
target_link_libraries(cpp-containers-paged_vector PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-paged_vector PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/paged_vector/paged_vector.cpp
)

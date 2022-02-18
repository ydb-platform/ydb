add_library(cpp-containers-stack_vector)
target_link_libraries(cpp-containers-stack_vector PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-stack_vector PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/stack_vector/stack_vec.cpp
)

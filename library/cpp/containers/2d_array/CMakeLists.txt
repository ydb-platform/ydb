add_library(cpp-containers-2d_array)
target_link_libraries(cpp-containers-2d_array PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-2d_array PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/2d_array/2d_array.cpp
)

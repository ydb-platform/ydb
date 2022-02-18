add_library(cpp-containers-ring_buffer)
target_link_libraries(cpp-containers-ring_buffer PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-ring_buffer PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/ring_buffer/ring_buffer.cpp
)

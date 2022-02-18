add_library(cpp-streams-zc_memory_input)
target_link_libraries(cpp-streams-zc_memory_input PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-streams-zc_memory_input PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/streams/zc_memory_input/zc_memory_input.cpp
)

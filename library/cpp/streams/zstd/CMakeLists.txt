add_library(cpp-streams-zstd)
target_link_libraries(cpp-streams-zstd PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-zstd
)
target_sources(cpp-streams-zstd PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/streams/zstd/zstd.cpp
)

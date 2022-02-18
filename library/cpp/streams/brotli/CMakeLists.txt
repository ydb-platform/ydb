add_library(cpp-streams-brotli)
target_link_libraries(cpp-streams-brotli PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-brotli-enc
  libs-brotli-dec
)
target_sources(cpp-streams-brotli PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/streams/brotli/brotli.cpp
)

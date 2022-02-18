add_library(cpp-http-io)
target_link_libraries(cpp-http-io PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-blockcodecs
  cpp-streams-brotli
  cpp-streams-bzip2
  cpp-streams-lzma
)
target_sources(cpp-http-io PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/http/io/chunk.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/io/compression.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/io/headers.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/io/stream.cpp
)

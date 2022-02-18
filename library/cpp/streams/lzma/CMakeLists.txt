add_library(cpp-streams-lzma)
target_link_libraries(cpp-streams-lzma PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-lzmasdk
)
target_sources(cpp-streams-lzma PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/streams/lzma/lzma.cpp
)

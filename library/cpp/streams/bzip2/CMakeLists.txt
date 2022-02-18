add_library(cpp-streams-bzip2)
target_link_libraries(cpp-streams-bzip2 PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-libbz2
)
target_sources(cpp-streams-bzip2 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/streams/bzip2/bzip2.cpp
)

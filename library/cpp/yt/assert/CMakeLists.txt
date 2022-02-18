add_library(cpp-yt-assert)
target_link_libraries(cpp-yt-assert PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-yt-assert PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/assert/assert.cpp
)

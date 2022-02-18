add_library(cpp-yt-memory)
target_link_libraries(cpp-yt-memory PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yt-assert
  cpp-yt-misc
  cpp-ytalloc-api
)
target_sources(cpp-yt-memory PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/memory/blob.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/memory/ref.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/memory/ref_tracked.cpp
)

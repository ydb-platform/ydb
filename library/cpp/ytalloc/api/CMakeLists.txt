add_library(cpp-ytalloc-api)
target_link_libraries(cpp-ytalloc-api PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yt-misc
)
target_sources(cpp-ytalloc-api PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/ytalloc/api/fallback.cpp
)

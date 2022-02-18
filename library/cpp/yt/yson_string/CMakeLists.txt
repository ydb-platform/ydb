add_library(cpp-yt-yson_string)
target_link_libraries(cpp-yt-yson_string PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yt-assert
  cpp-yt-coding
  cpp-yt-exception
  cpp-yt-string
  cpp-yt-memory
  cpp-yt-misc
)
target_sources(cpp-yt-yson_string PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/yson_string/convert.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/yson_string/string.cpp
)

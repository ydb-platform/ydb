add_library(cpp-yt-yson)
target_link_libraries(cpp-yt-yson PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yt-yson_string
)
target_sources(cpp-yt-yson PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/yson/consumer.cpp
)

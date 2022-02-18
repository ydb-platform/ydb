add_library(cpp-yt-string)
target_link_libraries(cpp-yt-string PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yt-assert
  cpp-yt-exception
  cpp-yt-misc
)
target_sources(cpp-yt-string PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/string/enum.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/string/guid.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/string/string.cpp
)

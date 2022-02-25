add_library(cpp-yt-misc)
target_link_libraries(cpp-yt-misc PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-farmhash
  cpp-yt-exception
)
target_sources(cpp-yt-misc PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/misc/guid.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/misc/source_location.cpp
)

add_library(cpp-yt-exception)
target_link_libraries(cpp-yt-exception PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-yt-exception PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yt/exception/exception.cpp
)

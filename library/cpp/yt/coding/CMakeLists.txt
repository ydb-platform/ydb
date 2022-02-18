add_library(cpp-yt-coding INTERFACE)
target_link_libraries(cpp-yt-coding INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-yt-exception
)

add_library(cpp-histogram-hdr)
target_link_libraries(cpp-histogram-hdr PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-hdr_histogram
)
target_sources(cpp-histogram-hdr PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/hdr/histogram.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/hdr/histogram_iter.cpp
)

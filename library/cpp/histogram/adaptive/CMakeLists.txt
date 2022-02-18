add_library(cpp-histogram-adaptive)
target_link_libraries(cpp-histogram-adaptive PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  histogram-adaptive-protos
)
target_sources(cpp-histogram-adaptive PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/adaptive/common.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/adaptive/adaptive_histogram.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/adaptive/block_histogram.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/adaptive/fixed_bin_histogram.cpp
)

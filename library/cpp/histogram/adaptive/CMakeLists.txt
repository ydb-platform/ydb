
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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

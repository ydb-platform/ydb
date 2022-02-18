add_library(abseil-cpp-absl-meta INTERFACE)
target_link_libraries(abseil-cpp-absl-meta INTERFACE
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-absl-base
)

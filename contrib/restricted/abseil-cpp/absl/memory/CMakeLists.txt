add_library(abseil-cpp-absl-memory INTERFACE)
target_link_libraries(abseil-cpp-absl-memory INTERFACE
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-absl-meta
)

add_library(abseil-cpp-absl-algorithm INTERFACE)
target_link_libraries(abseil-cpp-absl-algorithm INTERFACE
  contrib-libs-cxxsupp
  yutil
)

add_library(abseil-cpp-absl-utility INTERFACE)
target_link_libraries(abseil-cpp-absl-utility INTERFACE
  contrib-libs-cxxsupp
  yutil
)

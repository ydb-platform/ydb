add_library(abseil-cpp-absl-container INTERFACE)
target_link_libraries(abseil-cpp-absl-container INTERFACE
  contrib-libs-cxxsupp
  yutil
)

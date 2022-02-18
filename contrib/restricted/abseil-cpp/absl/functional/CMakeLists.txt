add_library(abseil-cpp-absl-functional INTERFACE)
target_link_libraries(abseil-cpp-absl-functional INTERFACE
  contrib-libs-cxxsupp
  yutil
)

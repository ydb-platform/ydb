add_library(abseil-cpp-absl-types INTERFACE)
target_link_libraries(abseil-cpp-absl-types INTERFACE
  contrib-libs-cxxsupp
  yutil
)

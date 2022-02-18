add_library(abseil-cpp-tstring-y_absl-algorithm INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-algorithm INTERFACE
  contrib-libs-cxxsupp
  yutil
)

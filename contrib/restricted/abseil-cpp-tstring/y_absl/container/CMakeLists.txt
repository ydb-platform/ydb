add_library(abseil-cpp-tstring-y_absl-container INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-container INTERFACE
  contrib-libs-cxxsupp
  yutil
)

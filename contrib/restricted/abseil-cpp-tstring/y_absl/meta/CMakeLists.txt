add_library(abseil-cpp-tstring-y_absl-meta INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-meta INTERFACE
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
)

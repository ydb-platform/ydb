add_library(abseil-cpp-tstring-y_absl-memory INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-memory INTERFACE
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-meta
)

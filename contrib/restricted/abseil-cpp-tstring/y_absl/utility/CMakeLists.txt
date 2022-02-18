add_library(abseil-cpp-tstring-y_absl-utility INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-utility INTERFACE
  contrib-libs-cxxsupp
  yutil
)

add_library(abseil-cpp-tstring-y_absl-functional INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-functional INTERFACE
  contrib-libs-cxxsupp
  yutil
)

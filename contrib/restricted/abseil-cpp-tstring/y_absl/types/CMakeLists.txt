add_library(abseil-cpp-tstring-y_absl-types INTERFACE)
target_link_libraries(abseil-cpp-tstring-y_absl-types INTERFACE
  contrib-libs-cxxsupp
  yutil
)

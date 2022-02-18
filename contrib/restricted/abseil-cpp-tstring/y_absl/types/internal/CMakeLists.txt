add_library(y_absl-types-internal INTERFACE)
target_link_libraries(y_absl-types-internal INTERFACE
  contrib-libs-cxxsupp
  yutil
)

add_library(absl-types-internal INTERFACE)
target_link_libraries(absl-types-internal INTERFACE
  contrib-libs-cxxsupp
  yutil
)

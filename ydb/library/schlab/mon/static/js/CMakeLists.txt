add_library(mon-static-js INTERFACE)
target_link_libraries(mon-static-js INTERFACE
  contrib-libs-cxxsupp
  yutil
)

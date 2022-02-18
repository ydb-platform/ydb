add_library(schlab-mon-static INTERFACE)
target_link_libraries(schlab-mon-static INTERFACE
  contrib-libs-cxxsupp
  yutil
)

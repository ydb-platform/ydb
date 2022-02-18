add_library(mon-static-css INTERFACE)
target_link_libraries(mon-static-css INTERFACE
  contrib-libs-cxxsupp
  yutil
)

add_library(common-math-lib INTERFACE)
target_link_libraries(common-math-lib INTERFACE
  contrib-libs-cxxsupp
  yutil
)

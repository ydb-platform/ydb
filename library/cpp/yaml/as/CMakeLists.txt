add_library(cpp-yaml-as INTERFACE)
target_link_libraries(cpp-yaml-as INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-yaml-cpp
)

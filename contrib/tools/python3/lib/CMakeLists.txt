add_library(tools-python3-lib INTERFACE)
target_link_libraries(tools-python3-lib INTERFACE
  contrib-libs-cxxsupp
  yutil
  tools-python3-src
  contrib-libs-cxxsupp
  yutil
  certs
  contrib-libs-cxxsupp
  yutil
  python3-src-Modules
)

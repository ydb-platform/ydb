add_library(internal-proxies-macro INTERFACE)
target_link_libraries(internal-proxies-macro INTERFACE
  contrib-libs-cxxsupp
  yutil
)

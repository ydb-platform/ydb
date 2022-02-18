add_library(contrib-libs-cxxsupp INTERFACE)
target_link_libraries(contrib-libs-cxxsupp INTERFACE
  libs-cxxsupp-libcxx
)

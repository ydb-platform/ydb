add_library(contrib-libs-sparsehash INTERFACE)
target_include_directories(contrib-libs-sparsehash INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/libs/sparsehash/src
)
target_link_libraries(contrib-libs-sparsehash INTERFACE
  contrib-libs-cxxsupp
  yutil
)

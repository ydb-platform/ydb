add_library(contrib-libs-llvm12 INTERFACE)
target_include_directories(contrib-libs-llvm12 INTERFACE
  ${CMAKE_BINARY_DIR}/contrib/libs/llvm12/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/include
)
target_link_libraries(contrib-libs-llvm12 INTERFACE
  contrib-libs-cxxsupp
  yutil
)

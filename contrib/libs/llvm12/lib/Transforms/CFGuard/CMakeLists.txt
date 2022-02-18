add_library(lib-Transforms-CFGuard)
target_include_directories(lib-Transforms-CFGuard PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/CFGuard
)
target_link_libraries(lib-Transforms-CFGuard PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-IR
  llvm12-lib-Support
)
target_sources(lib-Transforms-CFGuard PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/CFGuard/CFGuard.cpp
)

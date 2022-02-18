add_library(lib-Transforms-AggressiveInstCombine)
target_include_directories(lib-Transforms-AggressiveInstCombine PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/AggressiveInstCombine
)
target_link_libraries(lib-Transforms-AggressiveInstCombine PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-Analysis
  llvm12-lib-IR
  llvm12-lib-Support
  lib-Transforms-Utils
)
target_sources(lib-Transforms-AggressiveInstCombine PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/AggressiveInstCombine/AggressiveInstCombine.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/AggressiveInstCombine/TruncInstCombine.cpp
)

add_library(llvm12-lib-Linker)
target_include_directories(llvm12-lib-Linker PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Linker
)
target_link_libraries(llvm12-lib-Linker PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-IR
  llvm12-lib-Object
  llvm12-lib-Support
  lib-Transforms-Utils
)
target_sources(llvm12-lib-Linker PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Linker/IRMover.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Linker/LinkModules.cpp
)

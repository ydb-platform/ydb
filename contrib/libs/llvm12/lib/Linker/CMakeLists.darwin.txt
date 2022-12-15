
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(llvm12-lib-Linker)
target_compile_options(llvm12-lib-Linker PRIVATE
  -Wno-everything
)
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

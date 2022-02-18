add_library(lib-ExecutionEngine-MCJIT)
target_include_directories(lib-ExecutionEngine-MCJIT PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
)
target_link_libraries(lib-ExecutionEngine-MCJIT PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-ExecutionEngine
  lib-ExecutionEngine-RuntimeDyld
  llvm12-lib-IR
  llvm12-lib-Object
  llvm12-lib-Support
  llvm12-lib-Target
)
target_sources(lib-ExecutionEngine-MCJIT PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/MCJIT/MCJIT.cpp
)

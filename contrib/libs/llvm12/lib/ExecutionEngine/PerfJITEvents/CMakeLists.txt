add_library(lib-ExecutionEngine-PerfJITEvents)
target_include_directories(lib-ExecutionEngine-PerfJITEvents PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/PerfJITEvents
)
target_link_libraries(lib-ExecutionEngine-PerfJITEvents PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-CodeGen
  lib-DebugInfo-DWARF
  llvm12-lib-ExecutionEngine
  lib-ExecutionEngine-RuntimeDyld
  llvm12-lib-IR
  llvm12-lib-Object
  llvm12-lib-Support
)
target_sources(lib-ExecutionEngine-PerfJITEvents PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/PerfJITEvents/PerfJITEventListener.cpp
)

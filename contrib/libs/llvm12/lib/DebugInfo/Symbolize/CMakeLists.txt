add_library(lib-DebugInfo-Symbolize)
target_include_directories(lib-DebugInfo-Symbolize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/Symbolize
)
target_link_libraries(lib-DebugInfo-Symbolize PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  lib-DebugInfo-DWARF
  lib-DebugInfo-PDB
  llvm12-lib-Demangle
  llvm12-lib-Object
  llvm12-lib-Support
)
target_sources(lib-DebugInfo-Symbolize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/Symbolize/DIPrinter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/Symbolize/SymbolizableObjectFile.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/Symbolize/Symbolize.cpp
)

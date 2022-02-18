add_library(lib-ExecutionEngine-RuntimeDyld)
target_include_directories(lib-ExecutionEngine-RuntimeDyld PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld
)
target_link_libraries(lib-ExecutionEngine-RuntimeDyld PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-IR
  llvm12-lib-MC
  llvm12-lib-Object
  llvm12-lib-Support
)
target_sources(lib-ExecutionEngine-RuntimeDyld PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/JITSymbol.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/RTDyldMemoryManager.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/RuntimeDyld.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldCOFF.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldChecker.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldELF.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldMachO.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ExecutionEngine/RuntimeDyld/Targets/RuntimeDyldELFMips.cpp
)

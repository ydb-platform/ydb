add_library(Target-X86-Disassembler)
target_include_directories(Target-X86-Disassembler PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/llvm12/lib/Target/X86
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86/Disassembler
)
target_link_libraries(Target-X86-Disassembler PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  lib-MC-MCDisassembler
  llvm12-lib-Support
  Target-X86-TargetInfo
)
target_sources(Target-X86-Disassembler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86/Disassembler/X86Disassembler.cpp
)

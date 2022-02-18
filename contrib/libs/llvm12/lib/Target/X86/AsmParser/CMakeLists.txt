add_library(Target-X86-AsmParser)
target_include_directories(Target-X86-AsmParser PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/llvm12/lib/Target/X86
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86/AsmParser
)
target_link_libraries(Target-X86-AsmParser PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-MC
  lib-MC-MCParser
  llvm12-lib-Support
  Target-X86-MCTargetDesc
  Target-X86-TargetInfo
)
target_sources(Target-X86-AsmParser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86/AsmParser/X86AsmParser.cpp
)

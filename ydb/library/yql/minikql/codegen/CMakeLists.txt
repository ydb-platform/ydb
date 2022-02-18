add_library(yql-minikql-codegen)
target_link_libraries(yql-minikql-codegen PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-cxxsupp-builtins
  contrib-libs-re2
  llvm12-lib-IR
  lib-ExecutionEngine-MCJIT
  llvm12-lib-Linker
  lib-Target-X86
  Target-X86-AsmParser
  Target-X86-Disassembler
  lib-Transforms-IPO
  lib-Transforms-ObjCARC
  lib-ExecutionEngine-PerfJITEvents
)
target_sources(yql-minikql-codegen PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/minikql/codegen/codegen.cpp
)

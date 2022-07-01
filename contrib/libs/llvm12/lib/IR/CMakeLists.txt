
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(llvm12-lib-IR)
target_compile_options(llvm12-lib-IR PRIVATE
  -Wno-everything
)
target_include_directories(llvm12-lib-IR PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR
)
target_link_libraries(llvm12-lib-IR PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-BinaryFormat
  llvm12-lib-Remarks
  llvm12-lib-Support
)
target_sources(llvm12-lib-IR PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/AbstractCallSite.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/AsmWriter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Assumptions.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Attributes.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/AutoUpgrade.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/BasicBlock.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Comdat.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/ConstantFold.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/ConstantRange.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Constants.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Core.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DIBuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DataLayout.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DebugInfo.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DebugInfoMetadata.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DebugLoc.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DiagnosticHandler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DiagnosticInfo.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/DiagnosticPrinter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Dominators.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/FPEnv.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Function.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/GVMaterializer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Globals.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/IRBuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/IRPrintingPasses.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/InlineAsm.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Instruction.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Instructions.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/IntrinsicInst.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/LLVMContext.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/LLVMContextImpl.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/LLVMRemarkStreamer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/LegacyPassManager.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/MDBuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Mangler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Metadata.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Module.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/ModuleSummaryIndex.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Operator.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/OptBisect.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Pass.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/PassInstrumentation.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/PassManager.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/PassRegistry.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/PassTimingInfo.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/PrintPasses.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/ProfileSummary.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/PseudoProbe.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/ReplaceConstant.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/SafepointIRVerifier.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Statepoint.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/StructuralHash.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Type.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/TypeFinder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Use.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/User.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Value.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/ValueSymbolTable.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/IR/Verifier.cpp
)

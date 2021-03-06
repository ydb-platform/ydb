#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===- InstrOrderFile.h ---- Late IR instrumentation for order file ----===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_TRANSFORMS_INSTRORDERFILE_H
#define LLVM_TRANSFORMS_INSTRORDERFILE_H

#include "llvm/IR/PassManager.h"

namespace llvm {
class Module;

/// The instrumentation pass for recording function order.
class InstrOrderFilePass : public PassInfoMixin<InstrOrderFilePass> {
public:
  PreservedAnalyses run(Module &M, ModuleAnalysisManager &AM);
};

} // end namespace llvm

#endif // LLVM_TRANSFORMS_INSTRORDERFILE_H

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===- SCCP.cpp - Sparse Conditional Constant Propagation -------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
//
// \file
// This file implements sparse conditional constant propagation and merging:
//
// Specifically, this:
//   * Assumes values are constant unless proven otherwise
//   * Assumes BasicBlocks are dead unless proven otherwise
//   * Proves values to be constant, and replaces them with constants
//   * Proves conditional branches to be unconditional
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_TRANSFORMS_SCALAR_SCCP_H
#define LLVM_TRANSFORMS_SCALAR_SCCP_H

#include "llvm/ADT/STLExtras.h"
#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/PassManager.h"
#include "llvm/Transforms/Utils/PredicateInfo.h"

namespace llvm {

class PostDominatorTree;

/// This pass performs function-level constant propagation and merging.
class SCCPPass : public PassInfoMixin<SCCPPass> {
public:
  PreservedAnalyses run(Function &F, FunctionAnalysisManager &AM);
};

/// Helper struct for bundling up the analysis results per function for IPSCCP.
struct AnalysisResultsForFn {
  std::unique_ptr<PredicateInfo> PredInfo;
  DominatorTree *DT;
  PostDominatorTree *PDT;
};

bool runIPSCCP(Module &M, const DataLayout &DL,
               std::function<const TargetLibraryInfo &(Function &)> GetTLI,
               function_ref<AnalysisResultsForFn(Function &)> getAnalysis);
} // end namespace llvm

#endif // LLVM_TRANSFORMS_SCALAR_SCCP_H

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

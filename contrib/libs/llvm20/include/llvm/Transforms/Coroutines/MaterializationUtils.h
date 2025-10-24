#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===- MaterializationUtils.h - Utilities for doing materialization -------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "llvm/Transforms/Coroutines/SuspendCrossingInfo.h"

#ifndef LLVM_TRANSFORMS_COROUTINES_MATERIALIZATIONUTILS_H
#define LLVM_TRANSFORMS_COROUTINES_MATERIALIZATIONUTILS_H

namespace llvm {

namespace coro {

// True if I is trivially rematerialzable, e.g. InsertElementInst
bool isTriviallyMaterializable(Instruction &I);

// Performs rematerialization, invoked from buildCoroutineFrame.
void doRematerializations(Function &F, SuspendCrossingInfo &Checker,
                          std::function<bool(Instruction &)> IsMaterializable);

} // namespace coro

} // namespace llvm

#endif // LLVM_TRANSFORMS_COROUTINES_MATERIALIZATIONUTILS_H

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

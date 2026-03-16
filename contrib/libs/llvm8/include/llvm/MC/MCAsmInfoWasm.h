#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===-- llvm/MC/MCAsmInfoWasm.h - Wasm Asm info -----------------*- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_MC_MCASMINFOWASM_H
#define LLVM_MC_MCASMINFOWASM_H

#include "llvm/MC/MCAsmInfo.h"

namespace llvm {
class MCAsmInfoWasm : public MCAsmInfo {
  virtual void anchor();

protected:
  MCAsmInfoWasm();
};
} // namespace llvm

#endif

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

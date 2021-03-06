#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===- ELFObjHandler.h ------------------------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===-----------------------------------------------------------------------===/
///
/// This supports reading and writing of elf dynamic shared objects.
///
//===-----------------------------------------------------------------------===/

#ifndef LLVM_TOOLS_ELFABI_ELFOBJHANDLER_H
#define LLVM_TOOLS_ELFABI_ELFOBJHANDLER_H

#include "llvm/InterfaceStub/ELFStub.h"
#include "llvm/Object/ELFObjectFile.h"
#include "llvm/Object/ELFTypes.h"
#include "llvm/Support/FileSystem.h"

namespace llvm {

class MemoryBuffer;

namespace elfabi {

enum class ELFTarget { ELF32LE, ELF32BE, ELF64LE, ELF64BE };

/// Attempt to read a binary ELF file from a MemoryBuffer.
Expected<std::unique_ptr<ELFStub>> readELFFile(MemoryBufferRef Buf);

/// Attempt to write a binary ELF stub.
/// This function determines appropriate ELFType using the passed ELFTarget and
/// then writes a binary ELF stub to a specified file path.
///
/// @param FilePath File path for writing the ELF binary.
/// @param Stub Source ELFStub to generate a binary ELF stub from.
/// @param OutputFormat Target ELFType to write binary as.
/// @param WriteIfChanged Whether or not to preserve timestamp if
///        the output stays the same.
Error writeBinaryStub(StringRef FilePath, const ELFStub &Stub,
                      ELFTarget OutputFormat, bool WriteIfChanged = false);

} // end namespace elfabi
} // end namespace llvm

#endif // LLVM_TOOLS_ELFABI_ELFOBJHANDLER_H

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

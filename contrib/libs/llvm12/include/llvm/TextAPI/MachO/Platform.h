#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===- llvm/TextAPI/MachO/Platform.h - Platform -----------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
//
// Defines the Platforms supported by Tapi and helpers.
//
//===----------------------------------------------------------------------===//
#ifndef LLVM_TEXTAPI_MACHO_PLATFORM_H
#define LLVM_TEXTAPI_MACHO_PLATFORM_H

#include "llvm/ADT/SmallSet.h"
#include "llvm/BinaryFormat/MachO.h"

namespace llvm {
namespace MachO {

/// Defines the list of MachO platforms.
enum class PlatformKind : unsigned {
  unknown,
  macOS = MachO::PLATFORM_MACOS,
  iOS = MachO::PLATFORM_IOS,
  tvOS = MachO::PLATFORM_TVOS,
  watchOS = MachO::PLATFORM_WATCHOS,
  bridgeOS = MachO::PLATFORM_BRIDGEOS,
  macCatalyst = MachO::PLATFORM_MACCATALYST,
  iOSSimulator = MachO::PLATFORM_IOSSIMULATOR,
  tvOSSimulator = MachO::PLATFORM_TVOSSIMULATOR,
  watchOSSimulator = MachO::PLATFORM_WATCHOSSIMULATOR,
  driverKit = MachO::PLATFORM_DRIVERKIT,
};

using PlatformSet = SmallSet<PlatformKind, 3>;

PlatformKind mapToPlatformKind(PlatformKind Platform, bool WantSim);
PlatformKind mapToPlatformKind(const Triple &Target);
PlatformSet mapToPlatformSet(ArrayRef<Triple> Targets);
StringRef getPlatformName(PlatformKind Platform);

} // end namespace MachO.
} // end namespace llvm.

#endif // LLVM_TEXTAPI_MACHO_PLATFORM_H

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

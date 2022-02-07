// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "highwayhash/instruction_sets.h"
#include "highwayhash/arch_specific.h"

// Currently there are only specialized targets for X64; other architectures
// only use HH_TARGET_Portable, in which case Supported() just returns that.
#if HH_ARCH_X64

#include <atomic>

namespace highwayhash {

namespace {

bool IsBitSet(const uint32_t reg, const int index) {
  return (reg & (1U << index)) != 0;
}

// Returns the lower 32 bits of extended control register 0.
// Requires CPU support for "OSXSAVE" (see below).
uint32_t ReadXCR0() {
#if HH_MSC_VERSION
  return static_cast<uint32_t>(_xgetbv(0));
#else
  uint32_t xcr0, xcr0_high;
  const uint32_t index = 0;
  asm volatile(".byte 0x0F, 0x01, 0xD0"
               : "=a"(xcr0), "=d"(xcr0_high)
               : "c"(index));
  return xcr0;
#endif
}

// 0 iff not yet initialized by Supported().
// Not function-local => no compiler-generated locking.
std::atomic<TargetBits> supported_{0};

// Bits indicating which instruction set extensions are supported.
enum {
  kBitSSE = 1 << 0,
  kBitSSE2 = 1 << 1,
  kBitSSE3 = 1 << 2,
  kBitSSSE3 = 1 << 3,
  kBitSSE41 = 1 << 4,
  kBitSSE42 = 1 << 5,
  kBitAVX = 1 << 6,
  kBitAVX2 = 1 << 7,
  kBitFMA = 1 << 8,
  kBitLZCNT = 1 << 9,
  kBitBMI = 1 << 10,
  kBitBMI2 = 1 << 11,

  kGroupAVX2 = kBitAVX | kBitAVX2 | kBitFMA | kBitLZCNT | kBitBMI | kBitBMI2,
  kGroupSSE41 = kBitSSE | kBitSSE2 | kBitSSE3 | kBitSSSE3 | kBitSSE41
};

}  // namespace

TargetBits InstructionSets::Supported() {
  TargetBits supported = supported_.load(std::memory_order_acquire);
  // Already initialized, return that.
  if (HH_LIKELY(supported)) {
    return supported;
  }

  uint32_t flags = 0;
  uint32_t abcd[4];

  Cpuid(0, 0, abcd);
  const uint32_t max_level = abcd[0];

  // Standard feature flags
  Cpuid(1, 0, abcd);
  flags |= IsBitSet(abcd[3], 25) ? kBitSSE : 0;
  flags |= IsBitSet(abcd[3], 26) ? kBitSSE2 : 0;
  flags |= IsBitSet(abcd[2], 0) ? kBitSSE3 : 0;
  flags |= IsBitSet(abcd[2], 9) ? kBitSSSE3 : 0;
  flags |= IsBitSet(abcd[2], 19) ? kBitSSE41 : 0;
  flags |= IsBitSet(abcd[2], 20) ? kBitSSE42 : 0;
  flags |= IsBitSet(abcd[2], 12) ? kBitFMA : 0;
  flags |= IsBitSet(abcd[2], 28) ? kBitAVX : 0;
  const bool has_osxsave = IsBitSet(abcd[2], 27);

  // Extended feature flags
  Cpuid(0x80000001U, 0, abcd);
  flags |= IsBitSet(abcd[2], 5) ? kBitLZCNT : 0;

  // Extended features
  if (max_level >= 7) {
    Cpuid(7, 0, abcd);
    flags |= IsBitSet(abcd[1], 3) ? kBitBMI : 0;
    flags |= IsBitSet(abcd[1], 5) ? kBitAVX2 : 0;
    flags |= IsBitSet(abcd[1], 8) ? kBitBMI2 : 0;
  }

  // Verify OS support for XSAVE, without which XMM/YMM registers are not
  // preserved across context switches and are not safe to use.
  if (has_osxsave) {
    const uint32_t xcr0 = ReadXCR0();
    // XMM
    if ((xcr0 & 2) == 0) {
      flags &= ~(kBitSSE | kBitSSE2 | kBitSSE3 | kBitSSSE3 | kBitSSE41 |
                 kBitSSE42 | kBitAVX | kBitAVX2 | kBitFMA);
    }
    // YMM
    if ((xcr0 & 4) == 0) {
      flags &= ~(kBitAVX | kBitAVX2);
    }
  }

  // Also indicates "supported" has been initialized.
  supported = HH_TARGET_Portable;

  // Set target bit(s) if all their group's flags are all set.
  if ((flags & kGroupAVX2) == kGroupAVX2) {
    supported |= HH_TARGET_AVX2;
  }
  if ((flags & kGroupSSE41) == kGroupSSE41) {
    supported |= HH_TARGET_SSE41;
  }

  supported_.store(supported, std::memory_order_release);
  return supported;
}

}  // namespace highwayhash

#endif  // HH_ARCH_X64

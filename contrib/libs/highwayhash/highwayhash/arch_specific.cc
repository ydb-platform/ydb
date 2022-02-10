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

#include "highwayhash/arch_specific.h"

#include <stdint.h>

#if HH_ARCH_X64 && !HH_MSC_VERSION
#  include <cpuid.h>
#endif

#include <string.h>  // memcpy
#include <string>

namespace highwayhash {

const char* TargetName(const TargetBits target_bit) {
  switch (target_bit) {
    case HH_TARGET_Portable:
      return "Portable";
    case HH_TARGET_SSE41:
      return "SSE41";
    case HH_TARGET_AVX2:
      return "AVX2";
    default:
      return nullptr;  // zero, multiple, or unknown bits
  }
}

#if HH_ARCH_X64

void Cpuid(const uint32_t level, const uint32_t count,
           uint32_t* HH_RESTRICT abcd) {
#if HH_MSC_VERSION
  int regs[4];
  __cpuidex(regs, level, count);
  for (int i = 0; i < 4; ++i) {
    abcd[i] = regs[i];
  }
#else
  uint32_t a, b, c, d;
  __cpuid_count(level, count, a, b, c, d);
  abcd[0] = a;
  abcd[1] = b;
  abcd[2] = c;
  abcd[3] = d;
#endif
}

uint32_t ApicId() {
  uint32_t abcd[4];
  Cpuid(1, 0, abcd);
  return abcd[1] >> 24;  // ebx
}

namespace {

std::string BrandString() {
  char brand_string[49];
  uint32_t abcd[4];

  // Check if brand string is supported (it is on all reasonable Intel/AMD)
  Cpuid(0x80000000U, 0, abcd);
  if (abcd[0] < 0x80000004U) {
    return std::string();
  }

  for (int i = 0; i < 3; ++i) {
    Cpuid(0x80000002U + i, 0, abcd);
    memcpy(brand_string + i * 16, &abcd, sizeof(abcd));
  }
  brand_string[48] = 0;
  return brand_string;
}

double DetectInvariantCyclesPerSecond() {
  const std::string& brand_string = BrandString();
  // Brand strings include the maximum configured frequency. These prefixes are
  // defined by Intel CPUID documentation.
  const char* prefixes[3] = {"MHz", "GHz", "THz"};
  const double multipliers[3] = {1E6, 1E9, 1E12};
  for (size_t i = 0; i < 3; ++i) {
    const size_t pos_prefix = brand_string.find(prefixes[i]);
    if (pos_prefix != std::string::npos) {
      const size_t pos_space = brand_string.rfind(' ', pos_prefix - 1);
      if (pos_space != std::string::npos) {
        const std::string digits =
            brand_string.substr(pos_space + 1, pos_prefix - pos_space - 1);
        return std::stod(digits) * multipliers[i];
      }
    }
  }

  return 0.0;
}

}  // namespace

double InvariantCyclesPerSecond() {
  // Thread-safe caching - this is called several times.
  static const double cycles_per_second = DetectInvariantCyclesPerSecond();
  return cycles_per_second;
}

#endif  // HH_ARCH_X64

}  // namespace highwayhash

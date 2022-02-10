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

#include <stdio.h>

#ifdef HH_GOOGLETEST
#include "testing/base/public/gmock.h"
#include "testing/base/public/gunit.h"
#endif

#include "highwayhash/instruction_sets.h"
#include "highwayhash/vector_test_target.h"

namespace highwayhash {
namespace {

void NotifyFailure(const char* target, const size_t size) {
  const size_t lane_bits = (size & 0xFF) * 8;
  const size_t lane_index = size >> 8;
#ifdef HH_GOOGLETEST
  EXPECT_TRUE(false) << "VectorTest failed for " << target << " T=" << lane_bits
                     << ", lane " << lane_index;
#else
  printf("VectorTest failed for %10s T=%zu, lane=%zu\n", target, lane_bits,
         lane_index);
#endif
}

void RunTests() {
  const TargetBits tested = InstructionSets::RunAll<VectorTest>(&NotifyFailure);
  HH_TARGET_NAME::ForeachTarget(tested, [](const TargetBits target) {
    printf("%10s: done\n", TargetName(target));
  });
}

#ifdef HH_GOOGLETEST
TEST(VectorTest, Run) { RunTests(); }
#endif

}  // namespace
}  // namespace highwayhash

#ifndef HH_GOOGLETEST
int main(int argc, char* argv[]) {
  highwayhash::RunTests();
  return 0;
}
#endif

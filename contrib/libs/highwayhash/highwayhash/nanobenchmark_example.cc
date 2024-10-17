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

#include <cstddef>
#include <cstring>
#include <vector>

#include "highwayhash/nanobenchmark.h"
#include "highwayhash/os_specific.h"

namespace highwayhash {
namespace {

uint64_t RegionToMeasure(FuncInput size) {
  char from[8] = {static_cast<char>(size)};
  char to[8];
  memcpy(to, from, size);
  return to[0];
}

void TestMemcpy() {
  PinThreadToRandomCPU();
  static const size_t distribution[] = {3, 3, 4, 4, 7, 7, 8, 8};
  DurationsForInputs input_map = MakeDurationsForInputs(distribution, 10);
  MeasureDurations(&RegionToMeasure, &input_map);
  for (size_t i = 0; i < input_map.num_items; ++i) {
    input_map.items[i].PrintMedianAndVariability();
  }
}

}  // namespace
}  // namespace highwayhash

int main(int argc, char* argv[]) {
  highwayhash::TestMemcpy();
  return 0;
}

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

#ifndef HIGHWAYHASH_OS_SPECIFIC_H_
#define HIGHWAYHASH_OS_SPECIFIC_H_

#include <vector>

namespace highwayhash {

// Returns current wall-clock time [seconds].
double Now();

// Sets this thread's priority to the maximum. This should not be called on
// single-core systems. Requires elevated permissions. No effect on Linux
// because it increases runtime and variability (issue #19).
void RaiseThreadPriority();

// Returns CPU numbers in [0, N), where N is the number of bits in the
// thread's initial affinity (unaffected by any SetThreadAffinity).
std::vector<int> AvailableCPUs();

// Opaque.
struct ThreadAffinity;

// Caller must free() the return value.
ThreadAffinity* GetThreadAffinity();

// Restores a previous affinity returned by GetThreadAffinity.
void SetThreadAffinity(ThreadAffinity* affinity);

// Ensures the thread is running on the specified cpu, and no others.
// Useful for reducing nanobenchmark variability (fewer context switches).
// Uses SetThreadAffinity.
void PinThreadToCPU(const int cpu);

// Random choice of CPU avoids overloading any one core.
// Uses SetThreadAffinity.
void PinThreadToRandomCPU();

}  // namespace highwayhash

#endif  // HIGHWAYHASH_OS_SPECIFIC_H_

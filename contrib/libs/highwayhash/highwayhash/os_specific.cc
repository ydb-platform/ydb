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

#include "highwayhash/os_specific.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <random>

#include "highwayhash/arch_specific.h"

#if defined(_WIN32) || defined(_WIN64)
#define OS_WIN 1
#define NOMINMAX
#include <windows.h>
#else
#define OS_WIN 0
#endif

#ifdef __linux__
#define OS_LINUX 1
#include <sched.h>
#include <sys/time.h>
#else
#define OS_LINUX 0
#endif

#if defined(__MACH__) || defined(__APPLE__)
#define OS_MAC 1
#include <mach/mach.h>
#include <mach/mach_time.h>
#else
#define OS_MAC 0
#endif

#ifdef __FreeBSD__
#define OS_FREEBSD 1
#include <sys/cpuset.h>
#include <sys/param.h>
#include <unistd.h>
#else
#define OS_FREEBSD 0
#endif

namespace highwayhash {

#define CHECK(condition)                                       \
  while (!(condition)) {                                       \
    printf("os_specific CHECK failed at line %d\n", __LINE__); \
    abort();                                                   \
  }

double Now() {
#if OS_WIN
  LARGE_INTEGER counter;
  (void)QueryPerformanceCounter(&counter);
  static const double rcp_freq = []() {
    LARGE_INTEGER freq;
    (void)QueryPerformanceFrequency(&freq);
    return 1.0 / freq.QuadPart;
  }();
  return counter.QuadPart * rcp_freq;
#elif OS_MAC
  const auto t = mach_absolute_time();
  // On OSX/iOS platform the elapsed time is cpu time unit
  // We have to query the time base information to convert it back
  // See https://developer.apple.com/library/mac/qa/qa1398/_index.html
  static mach_timebase_info_data_t timebase;
  if (timebase.denom == 0) {
    (void)mach_timebase_info(&timebase);
  }
  return double(t) * timebase.numer / timebase.denom * 1E-9;
#else
  timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return t.tv_sec + t.tv_nsec * 1E-9;
#endif
}

void RaiseThreadPriority() {
#if OS_WIN
  BOOL ok = SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
  CHECK(ok);
  SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
  CHECK(ok);
#elif OS_LINUX
  // omit: SCHED_RR and SCHED_FIFO with sched_priority max, max-1 and max/2
  // lead to 2-3x runtime and higher variability!
#elif OS_FREEBSD
#elif OS_MAC
#else
#error "port"
#endif
}

struct ThreadAffinity {
#if OS_WIN
  DWORD_PTR mask;
#elif OS_LINUX
  cpu_set_t set;
#elif OS_FREEBSD
  cpuset_t set;
#endif
};

ThreadAffinity* GetThreadAffinity() {
  ThreadAffinity* affinity =
      static_cast<ThreadAffinity*>(malloc(sizeof(ThreadAffinity)));
#if OS_WIN
  DWORD_PTR system_affinity;
  const BOOL ok = GetProcessAffinityMask(GetCurrentProcess(), &affinity->mask,
                                         &system_affinity);
  CHECK(ok);
#elif OS_LINUX
  const pid_t pid = 0;  // current thread
  const int err = sched_getaffinity(pid, sizeof(cpu_set_t), &affinity->set);
  CHECK(err == 0);
#elif OS_FREEBSD
  const pid_t pid = getpid();  // current thread
  const int err = cpuset_getaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, pid,
                                     sizeof(cpuset_t), &affinity->set);
  CHECK(err == 0);
#endif
  return affinity;
}

namespace {

ThreadAffinity* OriginalThreadAffinity() {
  static ThreadAffinity* original = GetThreadAffinity();
  return original;
}

}  // namespace

void SetThreadAffinity(ThreadAffinity* affinity) {
  // Ensure original is initialized before changing.
  const ThreadAffinity* const original = OriginalThreadAffinity();
  CHECK(original != nullptr);

#if OS_WIN
  const HANDLE hThread = GetCurrentThread();
  const DWORD_PTR prev = SetThreadAffinityMask(hThread, affinity->mask);
  CHECK(prev != 0);
#elif OS_LINUX
  const pid_t pid = 0;  // current thread
  const int err = sched_setaffinity(pid, sizeof(cpu_set_t), &affinity->set);
  CHECK(err == 0);
#elif OS_FREEBSD
  const pid_t pid = getpid();  // current thread
  const int err = cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, pid,
                                     sizeof(cpuset_t), &affinity->set);
  CHECK(err == 0);
#elif OS_MAC
#else
#error "port"
#endif
}

std::vector<int> AvailableCPUs() {
  std::vector<int> cpus;
  cpus.reserve(64);
  const ThreadAffinity* const affinity = OriginalThreadAffinity();
#if OS_WIN
  for (int cpu = 0; cpu < 64; ++cpu) {
    if (affinity->mask & (1ULL << cpu)) {
      cpus.push_back(cpu);
    }
  }
#elif OS_LINUX
  for (size_t cpu = 0; cpu < sizeof(cpu_set_t) * 8; ++cpu) {
    if (CPU_ISSET(cpu, &affinity->set)) {
      cpus.push_back(cpu);
    }
  }
#elif OS_FREEBSD
  for (size_t cpu = 0; cpu < sizeof(cpuset_t) * 8; ++cpu) {
    if (CPU_ISSET(cpu, &affinity->set)) {
      cpus.push_back(cpu);
    }
  }
#elif OS_MAC
#else
#error "port"
#endif
  return cpus;
}

void PinThreadToCPU(const int cpu) {
  ThreadAffinity affinity;
#if OS_WIN
  affinity.mask = 1ULL << cpu;
#elif OS_LINUX
  CPU_ZERO(&affinity.set);
  CPU_SET(cpu, &affinity.set);
#elif OS_FREEBSD
  CPU_ZERO(&affinity.set);
  CPU_SET(cpu, &affinity.set);
#elif OS_MAC
#else
#error "port"
#endif
  SetThreadAffinity(&affinity);
}

void PinThreadToRandomCPU() {
  std::vector<int> cpus = AvailableCPUs();

  // Remove first two CPUs because interrupts are often pinned to them.
  CHECK(cpus.size() > 2);
  cpus.erase(cpus.begin(), cpus.begin() + 2);

  // Random choice to prevent burning up the same core.
  std::random_device device;
  std::ranlux48 generator(device());
  std::shuffle(cpus.begin(), cpus.end(), generator);
  const int cpu = cpus.front();

  PinThreadToCPU(cpu);

#if HH_ARCH_X64
  // After setting affinity, we should be running on the desired CPU.
  printf("Running on CPU #%d, APIC ID %02x\n", cpu, ApicId());
#else
  printf("Running on CPU #%d\n", cpu);
#endif
}

}  // namespace highwayhash

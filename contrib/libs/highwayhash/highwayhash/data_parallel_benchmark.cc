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

#include <cmath>
#include <cstdio>
#include <future>  //NOLINT
#include <set>
#include "testing/base/public/gunit.h"
#include "highwayhash/arch_specific.h"
#include "highwayhash/data_parallel.h"
#include "thread/threadpool.h"

namespace highwayhash {
namespace {

constexpr int kBenchmarkTasks = 1000000;

// Returns elapsed time [nanoseconds] for std::async.
double BenchmarkAsync(uint64_t* total) {
  const base::Time t0 = base::Now();
  std::atomic<uint64_t> sum1{0};
  std::atomic<uint64_t> sum2{0};

  std::vector<std::future<void>> futures;
  futures.reserve(kBenchmarkTasks);
  for (int i = 0; i < kBenchmarkTasks; ++i) {
    futures.push_back(std::async(
        [&sum1, &sum2](const int i) {
          sum1.fetch_add(i);
          sum2.fetch_add(1);
        },
        i));
  }

  for (auto& future : futures) {
    future.get();
  }

  const base::Time t1 = base::Now();
  *total = sum1.load() + sum2.load();
  return base::ToDoubleNanoseconds(t1 - t0);
}

// Returns elapsed time [nanoseconds] for (atomic) ThreadPool.
double BenchmarkPoolA(uint64_t* total) {
  const base::Time t0 = base::Now();
  std::atomic<uint64_t> sum1{0};
  std::atomic<uint64_t> sum2{0};

  ThreadPool pool;
  pool.Run(0, kBenchmarkTasks, [&sum1, &sum2](const int i) {
    sum1.fetch_add(i);
    sum2.fetch_add(1);
  });

  const base::Time t1 = base::Now();
  *total = sum1.load() + sum2.load();
  return base::ToDoubleNanoseconds(t1 - t0);
}

// Returns elapsed time [nanoseconds] for ::ThreadPool.
double BenchmarkPoolG(uint64_t* total) {
  const base::Time t0 = base::Now();
  std::atomic<uint64_t> sum1{0};
  std::atomic<uint64_t> sum2{0};

  {
    ::ThreadPool pool(std::thread::hardware_concurrency());
    pool.StartWorkers();
    for (int i = 0; i < kBenchmarkTasks; ++i) {
      pool.Schedule([&sum1, &sum2, i]() {
        sum1.fetch_add(i);
        sum2.fetch_add(1);
      });
    }
  }

  const base::Time t1 = base::Now();
  *total = sum1.load() + sum2.load();
  return base::ToDoubleNanoseconds(t1 - t0);
}

// Compares ThreadPool speed to std::async and ::ThreadPool.
TEST(DataParallelTest, Benchmarks) {
  uint64_t sum1, sum2, sum3;
  const double async_ns = BenchmarkAsync(&sum1);
  const double poolA_ns = BenchmarkPoolA(&sum2);
  const double poolG_ns = BenchmarkPoolG(&sum3);

  printf("Async %11.0f ns\nPoolA %11.0f ns\nPoolG %11.0f ns\n", async_ns,
         poolA_ns, poolG_ns);
  // baseline 20x, 10x with asan or msan, 5x with tsan
  EXPECT_GT(async_ns, poolA_ns * 4);
  // baseline 200x, 180x with asan, 70x with msan, 50x with tsan.
  EXPECT_GT(poolG_ns, poolA_ns * 20);

  // Should reach same result.
  EXPECT_EQ(sum1, sum2);
  EXPECT_EQ(sum2, sum3);
}

// Ensures multiple hardware threads are used (decided by the OS scheduler).
TEST(DataParallelTest, TestApicIds) {
  for (int num_threads = 1; num_threads <= std::thread::hardware_concurrency();
       ++num_threads) {
    ThreadPool pool(num_threads);

    std::mutex mutex;
    std::set<unsigned> ids;
    double total = 0.0;
    pool.Run(0, 2 * num_threads, [&mutex, &ids, &total](const int i) {
      // Useless computations to keep the processor busy so that threads
      // can't just reuse the same processor.
      double sum = 0.0;
      for (int rep = 0; rep < 900 * (i + 30); ++rep) {
        sum += pow(rep, 0.5);
      }

      mutex.lock();
      ids.insert(ApicId());
      total += sum;
      mutex.unlock();
    });

    // No core ID / APIC ID available
    if (num_threads > 1 && ids.size() == 1) {
      EXPECT_EQ(0, *ids.begin());
    } else {
      // (The Linux scheduler doesn't use all available HTs, but the
      // computations should at least keep most cores busy.)
      EXPECT_GT(ids.size() + 2, num_threads / 4);
    }

    // (Ensure the busy-work is not elided.)
    EXPECT_GT(total, 1E4);
  }
}

}  // namespace
}  // namespace highwayhash

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

#include <unistd.h>
#include <cstdint>

#include "testing/base/public/gunit.h"
#include "highwayhash/data_parallel.h"

namespace highwayhash {
namespace {

int PopulationCount(uint64_t bits) {
  int num_set = 0;
  while (bits != 0) {
    num_set += bits & 1;
    bits >>= 1;
  }
  return num_set;
}

std::atomic<int> func_counts{0};

void Func2() {
  usleep(200000);
  func_counts.fetch_add(4);
}

void Func3() {
  usleep(300000);
  func_counts.fetch_add(16);
}

void Func4() {
  usleep(400000);
  func_counts.fetch_add(256);
}

// Exercises the RunTasks feature (running arbitrary tasks/closures)
TEST(DataParallelTest, TestRunTasks) {
  ThreadPool pool(4);
  pool.RunTasks({Func2, Func3, Func4});
  EXPECT_EQ(276, func_counts.load());
}

// Ensures task parameter is in bounds, every parameter is reached,
// pool can be reused (multiple consecutive Run calls), pool can be destroyed
// (joining with its threads).
TEST(DataParallelTest, TestPool) {
  for (int num_threads = 1; num_threads <= 18; ++num_threads) {
    ThreadPool pool(num_threads);
    for (int num_tasks = 0; num_tasks < 32; ++num_tasks) {
      std::vector<int> mementos(num_tasks, 0);
      for (int begin = 0; begin < 32; ++begin) {
        std::fill(mementos.begin(), mementos.end(), 0);
        pool.Run(begin, begin + num_tasks,
                 [begin, num_tasks, &mementos](const int i) {
                   // Parameter is in the given range
                   EXPECT_GE(i, begin);
                   EXPECT_LT(i, begin + num_tasks);

                   // Store mementos to be sure we visited each i.
                   mementos.at(i - begin) = 1000 + i;
                 });
        for (int i = begin; i < begin + num_tasks; ++i) {
          EXPECT_EQ(1000 + i, mementos.at(i - begin));
        }
      }
    }
  }
}

TEST(DataParallelTest, TestRunRanges) {
  for (int num_threads = 1; num_threads <= 18; ++num_threads) {
    ThreadPool pool(num_threads);
    for (int num_tasks = 0; num_tasks < 32; ++num_tasks) {
      std::vector<int> mementos(num_tasks, 0);
      for (int begin = 0; begin < 32; ++begin) {
        std::fill(mementos.begin(), mementos.end(), 0);
        pool.RunRanges(begin, begin + num_tasks,
                       [begin, num_tasks, &mementos](const int chunk,
                                                     const uint32_t my_begin,
                                                     const uint32_t my_end) {
                         for (uint32_t i = my_begin; i < my_end; ++i) {
                           // Parameter is in the given range
                           EXPECT_GE(i, begin);
                           EXPECT_LT(i, begin + num_tasks);

                           // Store mementos to be sure we visited each i.
                           mementos.at(i - begin) = 1000 + i;
                         }
                       });
        for (int i = begin; i < begin + num_tasks; ++i) {
          EXPECT_EQ(1000 + i, mementos.at(i - begin));
        }
      }
    }
  }
}

// Ensures each of N threads processes exactly 1 of N tasks, i.e. the
// work distribution is perfectly fair for small counts.
TEST(DataParallelTest, TestSmallAssignments) {
  for (int num_threads = 1; num_threads <= 64; ++num_threads) {
    ThreadPool pool(num_threads);

    std::atomic<int> counter{0};
    // (Avoid mutex because it may perturb the worker thread scheduling)
    std::atomic<uint64_t> id_bits{0};

    pool.Run(0, num_threads, [&counter, num_threads, &id_bits](const int i) {
      const int id = counter.fetch_add(1);
      EXPECT_LT(id, num_threads);
      uint64_t bits = id_bits.load(std::memory_order_relaxed);
      while (!id_bits.compare_exchange_weak(bits, bits | (1ULL << id))) {
      }
    });

    const int num_participants = PopulationCount(id_bits.load());
    EXPECT_EQ(num_threads, num_participants);
  }
}

// Test payload for PerThread.
struct CheckUniqueIDs {
  bool IsNull() const { return false; }
  void Destroy() { id_bits = 0; }
  void Assimilate(const CheckUniqueIDs& victim) {
    // Cannot overlap because each PerThread has unique bits.
    EXPECT_EQ(0, id_bits & victim.id_bits);
    id_bits |= victim.id_bits;
  }

  uint64_t id_bits = 0;
};

// Ensures each thread has a PerThread instance, that they are successfully
// combined/reduced into a single result, and that reuse is possible after
// Destroy().
TEST(DataParallelTest, TestPerThread) {
  // We use a uint64_t bit array for convenience => no more than 64 threads.
  const int max_threads = std::min(64U, std::thread::hardware_concurrency());
  for (int num_threads = 1; num_threads <= max_threads; ++num_threads) {
    ThreadPool pool(num_threads);

    std::atomic<int> counter{0};
    pool.Run(0, num_threads, [&counter, num_threads](const int i) {
      const int id = counter.fetch_add(1);
      EXPECT_LT(id, num_threads);
      PerThread<CheckUniqueIDs>::Get().id_bits |= 1ULL << id;
    });

    // Verify each thread's bit is set.
    const uint64_t all_bits = PerThread<CheckUniqueIDs>::Reduce().id_bits;
    // Avoid shifting by 64 (undefined).
    const uint64_t expected =
        num_threads == 64 ? ~0ULL : (1ULL << num_threads) - 1;
    EXPECT_EQ(expected, all_bits);
    PerThread<CheckUniqueIDs>::Destroy();
  }
}

}  // namespace
}  // namespace highwayhash

// Copyright 2023 The gRPC Authors
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
#include <grpc/support/port_platform.h>

#include <stddef.h>

#include <memory>

#include <grpc/support/cpu.h>

#include "src/core/lib/event_engine/forkable.h"
#include "src/core/lib/event_engine/thread_pool/thread_pool.h"
#include "src/core/lib/event_engine/thread_pool/work_stealing_thread_pool.h"
#include "src/core/lib/gprpp/no_destruct.h"
#include "src/core/lib/gprpp/env.h"
#include "src/core/lib/gpr/string.h"

namespace grpc_event_engine {
namespace experimental {

namespace {
grpc_core::NoDestruct<ObjectGroupForkHandler> g_thread_pool_fork_manager;

class ThreadPoolForkCallbackMethods {
 public:
  static void Prefork() { g_thread_pool_fork_manager->Prefork(); }
  static void PostforkParent() { g_thread_pool_fork_manager->PostforkParent(); }
  static void PostforkChild() { g_thread_pool_fork_manager->PostforkChild(); }
};

size_t GetMaxPoolThreadFromEnv() {
  auto value = grpc_core::GetEnv("GRPC_MAX_THREADS_THREAD_POOL_ENV");
  if (!value.has_value()) return 0;
  int parse_succeeded = gpr_parse_nonnegative_int(value->c_str());

  if (parse_succeeded <= 0) {
     return 0;
  }
  return static_cast<size_t>(parse_succeeded);
}

}  // namespace

size_t ThreadPool::SetThreadsLimit(size_t count) {
    size_t prev = threads_limit_;
    const auto threads_limit_env = GetMaxPoolThreadFromEnv();
    if (threads_limit_env && threads_limit_env < count) {
        gpr_log(GPR_INFO, "Threads limit changed via env from %u to %u", count, threads_limit_env);
        count = threads_limit_env;
    }
    threads_limit_ = count;
    return prev;
}

size_t ThreadPool::GetThreadsLimit() { return threads_limit_; }

size_t ThreadPool::threads_limit_ = GetMaxPoolThreadFromEnv() ? GetMaxPoolThreadFromEnv() : grpc_core::Clamp(gpr_cpu_num_cores(), 2u, 32u);

std::shared_ptr<ThreadPool> MakeThreadPool(size_t reserve_threads) {
  auto thread_pool = std::make_shared<WorkStealingThreadPool>(reserve_threads);
  g_thread_pool_fork_manager->RegisterForkable(
      thread_pool, ThreadPoolForkCallbackMethods::Prefork,
      ThreadPoolForkCallbackMethods::PostforkParent,
      ThreadPoolForkCallbackMethods::PostforkChild);
  return thread_pool;
}

}  // namespace experimental
}  // namespace grpc_event_engine

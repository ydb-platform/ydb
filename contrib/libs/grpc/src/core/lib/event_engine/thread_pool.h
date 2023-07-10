/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef GRPC_CORE_LIB_EVENT_ENGINE_THREAD_POOL_H
#define GRPC_CORE_LIB_EVENT_ENGINE_THREAD_POOL_H

#include <grpc/support/port_platform.h>

#include <atomic>
#include <memory>
#include <queue>

#include "y_absl/base/thread_annotations.h"
#include "y_absl/functional/any_invocable.h"

#include "src/core/lib/event_engine/forkable.h"
#include "src/core/lib/gprpp/sync.h"

namespace grpc_event_engine {
namespace experimental {

class ThreadPool final : public grpc_event_engine::experimental::Forkable {
 public:
  explicit ThreadPool(int reserve_threads);
  // Ensures the thread pool is empty before destroying it.
  ~ThreadPool() override;

  void Add(y_absl::AnyInvocable<void()> callback);

  // Forkable
  // Ensures that the thread pool is empty before forking.
  void PrepareFork() override;
  void PostforkParent() override;
  void PostforkChild() override;

 private:
  class Queue {
   public:
    explicit Queue(int reserve_threads) : reserve_threads_(reserve_threads) {}
    bool Step();
    // Add a callback to the queue.
    // Return true if we should also spin up a new thread.
    bool Add(y_absl::AnyInvocable<void()> callback);
    void SetShutdown(bool is_shutdown);
    void SetForking(bool is_forking);

   private:

    grpc_core::Mutex mu_;
    grpc_core::CondVar cv_;
    std::queue<y_absl::AnyInvocable<void()>> callbacks_ Y_ABSL_GUARDED_BY(mu_);
    int threads_waiting_ Y_ABSL_GUARDED_BY(mu_) = 0;
    const int reserve_threads_;
    // Track shutdown and fork bits separately.
    // It's possible for a ThreadPool to initiate shut down while fork handlers
    // are running, and similarly possible for a fork event to occur during
    // shutdown.
    bool shutdown_ Y_ABSL_GUARDED_BY(mu_) = false;
    bool forking_ Y_ABSL_GUARDED_BY(mu_) = false;
  };

  class ThreadCount {
   public:
    void Add();
    void Remove();
    void BlockUntilThreadCount(int threads, const char* why);

   private:
    grpc_core::Mutex mu_;
    grpc_core::CondVar cv_;
    int threads_ Y_ABSL_GUARDED_BY(mu_) = 0;
  };

  struct State {
    explicit State(int reserve_threads) : queue(reserve_threads) {}
    Queue queue;
    ThreadCount thread_count;
    // After pool creation we use this to rate limit creation of threads to one
    // at a time.
    std::atomic<bool> currently_starting_one_thread{false};
  };

  using StatePtr = std::shared_ptr<State>;

  static void ThreadFunc(StatePtr state);
  // Start a new thread; throttled indicates whether the State::starting_thread
  // variable is being used to throttle this threads creation against others or
  // not: at thread pool startup we start several threads concurrently, but
  // after that we only start one at a time.
  static void StartThread(StatePtr state, bool throttled);
  void Postfork();

  const int reserve_threads_;
  const StatePtr state_ = std::make_shared<State>(reserve_threads_);
};

}  // namespace experimental
}  // namespace grpc_event_engine

#endif  // GRPC_CORE_LIB_EVENT_ENGINE_THREAD_POOL_H

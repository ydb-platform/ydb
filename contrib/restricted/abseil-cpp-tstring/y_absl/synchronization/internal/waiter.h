// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef Y_ABSL_SYNCHRONIZATION_INTERNAL_WAITER_H_
#define Y_ABSL_SYNCHRONIZATION_INTERNAL_WAITER_H_

#include "y_absl/base/config.h"

#ifdef _WIN32
#include <sdkddkver.h>
#else
#include <pthread.h>
#endif

#ifdef __linux__
#include <linux/futex.h>
#endif

#ifdef Y_ABSL_HAVE_SEMAPHORE_H
#include <semaphore.h>
#endif

#include <atomic>
#include <cstdint>

#include "y_absl/base/internal/thread_identity.h"
#include "y_absl/synchronization/internal/futex.h"
#include "y_absl/synchronization/internal/kernel_timeout.h"

// May be chosen at compile time via -DABSL_FORCE_WAITER_MODE=<index>
#define Y_ABSL_WAITER_MODE_FUTEX 0
#define Y_ABSL_WAITER_MODE_SEM 1
#define Y_ABSL_WAITER_MODE_CONDVAR 2
#define Y_ABSL_WAITER_MODE_WIN32 3

#if defined(Y_ABSL_FORCE_WAITER_MODE)
#define Y_ABSL_WAITER_MODE Y_ABSL_FORCE_WAITER_MODE
#elif defined(_WIN32) && _WIN32_WINNT >= _WIN32_WINNT_VISTA
#define Y_ABSL_WAITER_MODE Y_ABSL_WAITER_MODE_WIN32
#elif defined(Y_ABSL_INTERNAL_HAVE_FUTEX)
#define Y_ABSL_WAITER_MODE Y_ABSL_WAITER_MODE_FUTEX
#elif defined(Y_ABSL_HAVE_SEMAPHORE_H)
#define Y_ABSL_WAITER_MODE Y_ABSL_WAITER_MODE_SEM
#else
#define Y_ABSL_WAITER_MODE Y_ABSL_WAITER_MODE_CONDVAR
#endif

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace synchronization_internal {

// Waiter is an OS-specific semaphore.
class Waiter {
 public:
  // Prepare any data to track waits.
  Waiter();

  // Not copyable or movable
  Waiter(const Waiter&) = delete;
  Waiter& operator=(const Waiter&) = delete;

  // Blocks the calling thread until a matching call to `Post()` or
  // `t` has passed. Returns `true` if woken (`Post()` called),
  // `false` on timeout.
  bool Wait(KernelTimeout t);

  // Restart the caller of `Wait()` as with a normal semaphore.
  void Post();

  // If anyone is waiting, wake them up temporarily and cause them to
  // call `MaybeBecomeIdle()`. They will then return to waiting for a
  // `Post()` or timeout.
  void Poke();

  // Returns the Waiter associated with the identity.
  static Waiter* GetWaiter(base_internal::ThreadIdentity* identity) {
    static_assert(
        sizeof(Waiter) <= sizeof(base_internal::ThreadIdentity::WaiterState),
        "Insufficient space for Waiter");
    return reinterpret_cast<Waiter*>(identity->waiter_state.data);
  }

  // How many periods to remain idle before releasing resources
#ifndef Y_ABSL_HAVE_THREAD_SANITIZER
  static constexpr int kIdlePeriods = 60;
#else
  // Memory consumption under ThreadSanitizer is a serious concern,
  // so we release resources sooner. The value of 1 leads to 1 to 2 second
  // delay before marking a thread as idle.
  static const int kIdlePeriods = 1;
#endif

 private:
  // The destructor must not be called since Mutex/CondVar
  // can use PerThreadSem/Waiter after the thread exits.
  // Waiter objects are embedded in ThreadIdentity objects,
  // which are reused via a freelist and are never destroyed.
  ~Waiter() = delete;

#if Y_ABSL_WAITER_MODE == Y_ABSL_WAITER_MODE_FUTEX
  // Futexes are defined by specification to be 32-bits.
  // Thus std::atomic<int32_t> must be just an int32_t with lockfree methods.
  std::atomic<int32_t> futex_;
  static_assert(sizeof(int32_t) == sizeof(futex_), "Wrong size for futex");

#elif Y_ABSL_WAITER_MODE == Y_ABSL_WAITER_MODE_CONDVAR
  // REQUIRES: mu_ must be held.
  void InternalCondVarPoke();

  pthread_mutex_t mu_;
  pthread_cond_t cv_;
  int waiter_count_;
  int wakeup_count_;  // Unclaimed wakeups.

#elif Y_ABSL_WAITER_MODE == Y_ABSL_WAITER_MODE_SEM
  sem_t sem_;
  // This seems superfluous, but for Poke() we need to cause spurious
  // wakeups on the semaphore. Hence we can't actually use the
  // semaphore's count.
  std::atomic<int> wakeups_;

#elif Y_ABSL_WAITER_MODE == Y_ABSL_WAITER_MODE_WIN32
  // WinHelper - Used to define utilities for accessing the lock and
  // condition variable storage once the types are complete.
  class WinHelper;

  // REQUIRES: WinHelper::GetLock(this) must be held.
  void InternalCondVarPoke();

  // We can't include Windows.h in our headers, so we use aligned character
  // buffers to define the storage of SRWLOCK and CONDITION_VARIABLE.
  // SRW locks and condition variables do not need to be explicitly destroyed.
  // https://docs.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-initializesrwlock
  // https://stackoverflow.com/questions/28975958/why-does-windows-have-no-deleteconditionvariable-function-to-go-together-with
  alignas(void*) unsigned char mu_storage_[sizeof(void*)];
  alignas(void*) unsigned char cv_storage_[sizeof(void*)];
  int waiter_count_;
  int wakeup_count_;

#else
  #error Unknown Y_ABSL_WAITER_MODE
#endif
};

}  // namespace synchronization_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_SYNCHRONIZATION_INTERNAL_WAITER_H_

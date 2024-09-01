// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef GOOGLE_PROTOBUF_SRC_GOOGLE_PROTOBUF_ARENAZ_SAMPLER_H__
#define GOOGLE_PROTOBUF_SRC_GOOGLE_PROTOBUF_ARENAZ_SAMPLER_H__

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

#include <google/protobuf/stubs/port.h>

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace internal {

#if defined(PROTOBUF_ARENAZ_SAMPLE)
struct ThreadSafeArenaStats;
void RecordAllocateSlow(ThreadSafeArenaStats* info, size_t used,
                        size_t allocated, size_t wasted);
// Stores information about a sampled thread safe arena.  All mutations to this
// *must* be made through `Record*` functions below.  All reads from this *must*
// only occur in the callback to `ThreadSafeArenazSampler::Iterate`.
struct ThreadSafeArenaStats
    : public y_absl::profiling_internal::Sample<ThreadSafeArenaStats> {
  // Constructs the object but does not fill in any fields.
  ThreadSafeArenaStats();
  ~ThreadSafeArenaStats();

  // Puts the object into a clean state, fills in the logically `const` members,
  // blocking for any readers that are currently sampling the object.  The
  // 'stride' parameter is the number of ThreadSafeArenas that were instantiated
  // between this sample and the previous one.
  void PrepareForSampling(arc_i64 stride)
      Y_ABSL_EXCLUSIVE_LOCKS_REQUIRED(init_mu);

  // These fields are mutated by the various Record* APIs and need to be
  // thread-safe.
  struct BlockStats {
    std::atomic<int> num_allocations;
    std::atomic<size_t> bytes_allocated;
    std::atomic<size_t> bytes_used;
    std::atomic<size_t> bytes_wasted;

    void PrepareForSampling();
  };

  // block_histogram is a kBlockHistogramBins sized histogram.  The zeroth bin
  // stores info about blocks of size \in [1, 1 << kLogMaxSizeForBinZero]. Bin
  // i, where i > 0, stores info for blocks of size \in (max_size_bin (i-1),
  // 1 << (kLogMaxSizeForBinZero + i)].  The final bin stores info about blocks
  // of size \in [kMaxSizeForPenultimateBin + 1,
  // std::numeric_limits<size_t>::max()].
  static constexpr size_t kBlockHistogramBins = 15;
  static constexpr size_t kLogMaxSizeForBinZero = 7;
  static constexpr size_t kMaxSizeForBinZero = (1 << kLogMaxSizeForBinZero);
  static constexpr size_t kMaxSizeForPenultimateBin =
      1 << (kLogMaxSizeForBinZero + kBlockHistogramBins - 2);
  std::array<BlockStats, kBlockHistogramBins> block_histogram;

  // Records the largest block allocated for the arena.
  std::atomic<size_t> max_block_size;
  // Bit `i` is set to 1 indicates that a thread with `tid % 63 = i` accessed
  // the underlying arena.  We use `% 63` as a rudimentary hash to ensure some
  // bit mixing for thread-ids; `% 64` would only grab the low bits and might
  // create sampling artifacts.
  std::atomic<arc_ui64> thread_ids;

  // All of the fields below are set by `PrepareForSampling`, they must not
  // be mutated in `Record*` functions.  They are logically `const` in that
  // sense. These are guarded by init_mu, but that is not externalized to
  // clients, who can only read them during
  // `ThreadSafeArenazSampler::Iterate` which will hold the lock.
  static constexpr int kMaxStackDepth = 64;
  arc_i32 depth;
  void* stack[kMaxStackDepth];
  static void RecordAllocateStats(ThreadSafeArenaStats* info, size_t used,
                                  size_t allocated, size_t wasted) {
    if (PROTOBUF_PREDICT_TRUE(info == nullptr)) return;
    RecordAllocateSlow(info, used, allocated, wasted);
  }

  // Returns the bin for the provided size.
  static size_t FindBin(size_t bytes);

  // Returns the min and max bytes that can be stored in the histogram for
  // blocks in the provided bin.
  static std::pair<size_t, size_t> MinMaxBlockSizeForBin(size_t bin);
};

struct SamplingState {
  // Number of ThreadSafeArenas that should be instantiated before the next
  // ThreadSafeArena is sampled.  This variable is decremented with each
  // instantiation.
  arc_i64 next_sample;
  // When we make a sampling decision, we record that distance between from the
  // previous sample so we can weight each sample.  'distance' here is the
  // number of instantiations of ThreadSafeArena.
  arc_i64 sample_stride;
};

ThreadSafeArenaStats* SampleSlow(SamplingState& sampling_state);
void UnsampleSlow(ThreadSafeArenaStats* info);

class ThreadSafeArenaStatsHandle {
 public:
  explicit ThreadSafeArenaStatsHandle() = default;
  explicit ThreadSafeArenaStatsHandle(ThreadSafeArenaStats* info)
      : info_(info) {}

  ~ThreadSafeArenaStatsHandle() {
    if (PROTOBUF_PREDICT_TRUE(info_ == nullptr)) return;
    UnsampleSlow(info_);
  }

  ThreadSafeArenaStatsHandle(ThreadSafeArenaStatsHandle&& other) noexcept
      : info_(y_absl::exchange(other.info_, nullptr)) {}

  ThreadSafeArenaStatsHandle& operator=(
      ThreadSafeArenaStatsHandle&& other) noexcept {
    if (PROTOBUF_PREDICT_FALSE(info_ != nullptr)) {
      UnsampleSlow(info_);
    }
    info_ = y_absl::exchange(other.info_, nullptr);
    return *this;
  }

  ThreadSafeArenaStats* MutableStats() { return info_; }

  friend void swap(ThreadSafeArenaStatsHandle& lhs,
                   ThreadSafeArenaStatsHandle& rhs) {
    std::swap(lhs.info_, rhs.info_);
  }

  friend class ThreadSafeArenaStatsHandlePeer;

 private:
  ThreadSafeArenaStats* info_ = nullptr;
};

using ThreadSafeArenazSampler =
    ::y_absl::profiling_internal::SampleRecorder<ThreadSafeArenaStats>;

extern PROTOBUF_THREAD_LOCAL SamplingState global_sampling_state;

// Returns an RAII sampling handle that manages registration and unregistation
// with the global sampler.
inline ThreadSafeArenaStatsHandle Sample() {
  if (PROTOBUF_PREDICT_TRUE(--global_sampling_state.next_sample > 0)) {
    return ThreadSafeArenaStatsHandle(nullptr);
  }
  return ThreadSafeArenaStatsHandle(SampleSlow(global_sampling_state));
}

#else

using SamplingState = arc_i64;

struct ThreadSafeArenaStats {
  static void RecordAllocateStats(ThreadSafeArenaStats*, size_t /*requested*/,
                                  size_t /*allocated*/, size_t /*wasted*/) {}
};

ThreadSafeArenaStats* SampleSlow(SamplingState& next_sample);
void UnsampleSlow(ThreadSafeArenaStats* info);

class ThreadSafeArenaStatsHandle {
 public:
  explicit ThreadSafeArenaStatsHandle() = default;
  explicit ThreadSafeArenaStatsHandle(ThreadSafeArenaStats*) {}

  void RecordReset() {}

  ThreadSafeArenaStats* MutableStats() { return nullptr; }

  friend void swap(ThreadSafeArenaStatsHandle&, ThreadSafeArenaStatsHandle&) {}

 private:
  friend class ThreadSafeArenaStatsHandlePeer;
};

class ThreadSafeArenazSampler {
 public:
  void Unregister(ThreadSafeArenaStats*) {}
  void SetMaxSamples(arc_i32) {}
};

// Returns an RAII sampling handle that manages registration and unregistation
// with the global sampler.
inline ThreadSafeArenaStatsHandle Sample() {
  return ThreadSafeArenaStatsHandle(nullptr);
}
#endif  // defined(PROTOBUF_ARENAZ_SAMPLE)

// Returns a global Sampler.
ThreadSafeArenazSampler& GlobalThreadSafeArenazSampler();

using ThreadSafeArenazConfigListener = void (*)();
void SetThreadSafeArenazConfigListener(ThreadSafeArenazConfigListener l);

// Enables or disables sampling for thread safe arenas.
void SetThreadSafeArenazEnabled(bool enabled);
void SetThreadSafeArenazEnabledInternal(bool enabled);

// Returns true if sampling is on, false otherwise.
bool IsThreadSafeArenazEnabled();

// Sets the rate at which thread safe arena will be sampled.
void SetThreadSafeArenazSampleParameter(arc_i32 rate);
void SetThreadSafeArenazSampleParameterInternal(arc_i32 rate);

// Returns the rate at which thread safe arena will be sampled.
arc_i32 ThreadSafeArenazSampleParameter();

// Sets a soft max for the number of samples that will be kept.
void SetThreadSafeArenazMaxSamples(arc_i32 max);
void SetThreadSafeArenazMaxSamplesInternal(arc_i32 max);

// Returns the max number of samples that will be kept.
size_t ThreadSafeArenazMaxSamples();

// Sets the current value for when arenas should be next sampled.
void SetThreadSafeArenazGlobalNextSample(arc_i64 next_sample);

}  // namespace internal
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"
#endif  // GOOGLE_PROTOBUF_SRC_PROTOBUF_ARENAZ_SAMPLER_H__

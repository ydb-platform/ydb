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

#include "google/protobuf/arenaz_sampler.h"

#include <atomic>
#include <cstdint>
#include <limits>
#include <utility>


// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace internal {

ThreadSafeArenazSampler& GlobalThreadSafeArenazSampler() {
  static auto* sampler = new ThreadSafeArenazSampler();
  return *sampler;
}

void UnsampleSlow(ThreadSafeArenaStats* info) {
  GlobalThreadSafeArenazSampler().Unregister(info);
}

#if defined(PROTOBUF_ARENAZ_SAMPLE)
namespace {

PROTOBUF_CONSTINIT std::atomic<bool> g_arenaz_enabled{true};
PROTOBUF_CONSTINIT std::atomic<arc_i32> g_arenaz_sample_parameter{1 << 10};
PROTOBUF_CONSTINIT std::atomic<ThreadSafeArenazConfigListener>
    g_arenaz_config_listener{nullptr};
PROTOBUF_THREAD_LOCAL y_absl::profiling_internal::ExponentialBiased
    g_exponential_biased_generator;

void TriggerThreadSafeArenazConfigListener() {
  auto* listener = g_arenaz_config_listener.load(std::memory_order_acquire);
  if (listener != nullptr) listener();
}

}  // namespace

PROTOBUF_THREAD_LOCAL SamplingState global_sampling_state = {
    /*next_sample=*/0, /*sample_stride=*/0};

ThreadSafeArenaStats::ThreadSafeArenaStats() { PrepareForSampling(0); }
ThreadSafeArenaStats::~ThreadSafeArenaStats() = default;

void ThreadSafeArenaStats::BlockStats::PrepareForSampling() {
  num_allocations.store(0, std::memory_order_relaxed);
  bytes_allocated.store(0, std::memory_order_relaxed);
  bytes_used.store(0, std::memory_order_relaxed);
  bytes_wasted.store(0, std::memory_order_relaxed);
}

void ThreadSafeArenaStats::PrepareForSampling(arc_i64 stride) {
  for (auto& blockstats : block_histogram) blockstats.PrepareForSampling();
  max_block_size.store(0, std::memory_order_relaxed);
  thread_ids.store(0, std::memory_order_relaxed);
  weight = stride;
  // The inliner makes hardcoded skip_count difficult (especially when combined
  // with LTO).  We use the ability to exclude stacks by regex when encoding
  // instead.
  depth = y_absl::GetStackTrace(stack, kMaxStackDepth, /* skip_count= */ 0);
}

size_t ThreadSafeArenaStats::FindBin(size_t bytes) {
  if (bytes <= kMaxSizeForBinZero) return 0;
  if (bytes <= kMaxSizeForPenultimateBin) {
    // y_absl::bit_width() returns one plus the base-2 logarithm of x, with any
    // fractional part discarded.
    return y_absl::bit_width(y_absl::bit_ceil(bytes)) - kLogMaxSizeForBinZero - 1;
  }
  return kBlockHistogramBins - 1;
}

std::pair<size_t, size_t> ThreadSafeArenaStats::MinMaxBlockSizeForBin(
    size_t bin) {
  Y_ABSL_ASSERT(bin < kBlockHistogramBins);
  if (bin == 0) return {1, kMaxSizeForBinZero};
  if (bin < kBlockHistogramBins - 1) {
    return {(1 << (kLogMaxSizeForBinZero + bin - 1)) + 1,
            1 << (kLogMaxSizeForBinZero + bin)};
  }
  return {kMaxSizeForPenultimateBin + 1, std::numeric_limits<size_t>::max()};
}

void RecordAllocateSlow(ThreadSafeArenaStats* info, size_t used,
                        size_t allocated, size_t wasted) {
  // Update the allocated bytes for the current block.
  ThreadSafeArenaStats::BlockStats& curr =
      info->block_histogram[ThreadSafeArenaStats::FindBin(allocated)];
  curr.bytes_allocated.fetch_add(allocated, std::memory_order_relaxed);
  curr.num_allocations.fetch_add(1, std::memory_order_relaxed);

  // Update the used and wasted bytes for the previous block.
  ThreadSafeArenaStats::BlockStats& prev =
      info->block_histogram[ThreadSafeArenaStats::FindBin(used + wasted)];
  prev.bytes_used.fetch_add(used, std::memory_order_relaxed);
  prev.bytes_wasted.fetch_add(wasted, std::memory_order_relaxed);

  if (info->max_block_size.load(std::memory_order_relaxed) < allocated) {
    info->max_block_size.store(allocated, std::memory_order_relaxed);
  }
  const arc_ui64 tid = 1ULL << (GetCachedTID() % 63);
  info->thread_ids.fetch_or(tid, std::memory_order_relaxed);
}

ThreadSafeArenaStats* SampleSlow(SamplingState& sampling_state) {
  bool first = sampling_state.next_sample < 0;
  const arc_i64 next_stride = g_exponential_biased_generator.GetStride(
      g_arenaz_sample_parameter.load(std::memory_order_relaxed));
  // Small values of interval are equivalent to just sampling next time.
  Y_ABSL_ASSERT(next_stride >= 1);
  sampling_state.next_sample = next_stride;
  const arc_i64 old_stride =
      y_absl::exchange(sampling_state.sample_stride, next_stride);

  // g_arenaz_enabled can be dynamically flipped, we need to set a threshold low
  // enough that we will start sampling in a reasonable time, so we just use the
  // default sampling rate.
  if (!g_arenaz_enabled.load(std::memory_order_relaxed)) return nullptr;
  // We will only be negative on our first count, so we should just retry in
  // that case.
  if (first) {
    if (PROTOBUF_PREDICT_TRUE(--sampling_state.next_sample > 0)) return nullptr;
    return SampleSlow(sampling_state);
  }

  return GlobalThreadSafeArenazSampler().Register(old_stride);
}

void SetThreadSafeArenazConfigListener(ThreadSafeArenazConfigListener l) {
  g_arenaz_config_listener.store(l, std::memory_order_release);
}

bool IsThreadSafeArenazEnabled() {
  return g_arenaz_enabled.load(std::memory_order_acquire);
}

void SetThreadSafeArenazEnabled(bool enabled) {
  SetThreadSafeArenazEnabledInternal(enabled);
  TriggerThreadSafeArenazConfigListener();
}

void SetThreadSafeArenazEnabledInternal(bool enabled) {
  g_arenaz_enabled.store(enabled, std::memory_order_release);
}

void SetThreadSafeArenazSampleParameter(arc_i32 rate) {
  SetThreadSafeArenazSampleParameterInternal(rate);
  TriggerThreadSafeArenazConfigListener();
}

void SetThreadSafeArenazSampleParameterInternal(arc_i32 rate) {
  if (rate > 0) {
    g_arenaz_sample_parameter.store(rate, std::memory_order_release);
  } else {
    Y_ABSL_RAW_LOG(ERROR, "Invalid thread safe arenaz sample rate: %lld",
                 static_cast<long long>(rate));  // NOLINT(runtime/int)
  }
}

arc_i32 ThreadSafeArenazSampleParameter() {
  return g_arenaz_sample_parameter.load(std::memory_order_relaxed);
}

void SetThreadSafeArenazMaxSamples(arc_i32 max) {
  SetThreadSafeArenazMaxSamplesInternal(max);
  TriggerThreadSafeArenazConfigListener();
}

void SetThreadSafeArenazMaxSamplesInternal(arc_i32 max) {
  if (max > 0) {
    GlobalThreadSafeArenazSampler().SetMaxSamples(max);
  } else {
    Y_ABSL_RAW_LOG(ERROR, "Invalid thread safe arenaz max samples: %lld",
                 static_cast<long long>(max));  // NOLINT(runtime/int)
  }
}

size_t ThreadSafeArenazMaxSamples() {
  return GlobalThreadSafeArenazSampler().GetMaxSamples();
}

void SetThreadSafeArenazGlobalNextSample(arc_i64 next_sample) {
  if (next_sample >= 0) {
    global_sampling_state.next_sample = next_sample;
    global_sampling_state.sample_stride = next_sample;
  } else {
    Y_ABSL_RAW_LOG(ERROR, "Invalid thread safe arenaz next sample: %lld",
                 static_cast<long long>(next_sample));  // NOLINT(runtime/int)
  }
}

#else
ThreadSafeArenaStats* SampleSlow(arc_i64* next_sample) {
  *next_sample = std::numeric_limits<arc_i64>::max();
  return nullptr;
}

void SetThreadSafeArenazConfigListener(ThreadSafeArenazConfigListener) {}
void SetThreadSafeArenazEnabled(bool enabled) {}
void SetThreadSafeArenazEnabledInternal(bool enabled) {}
bool IsThreadSafeArenazEnabled() { return false; }
void SetThreadSafeArenazSampleParameter(arc_i32 rate) {}
void SetThreadSafeArenazSampleParameterInternal(arc_i32 rate) {}
arc_i32 ThreadSafeArenazSampleParameter() { return 0; }
void SetThreadSafeArenazMaxSamples(arc_i32 max) {}
void SetThreadSafeArenazMaxSamplesInternal(arc_i32 max) {}
size_t ThreadSafeArenazMaxSamples() { return 0; }
void SetThreadSafeArenazGlobalNextSample(arc_i64 next_sample) {}
#endif  // defined(PROTOBUF_ARENAZ_SAMPLE)

}  // namespace internal
}  // namespace protobuf
}  // namespace google

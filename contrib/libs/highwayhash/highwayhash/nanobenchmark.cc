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

#include "highwayhash/nanobenchmark.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <map>
#include <random>
#include <vector>

#include <stddef.h>

#include "highwayhash/os_specific.h"
#include "highwayhash/robust_statistics.h"
#include "highwayhash/tsc_timer.h"

namespace highwayhash {
namespace {

// Enables sanity checks that verify correct operation at the cost of
// longer benchmark runs.
#ifndef NANOBENCHMARK_ENABLE_CHECKS
#define NANOBENCHMARK_ENABLE_CHECKS 0
#endif

#define NANOBENCHMARK_CHECK_ALWAYS(condition)                    \
  while (!(condition)) {                                         \
    printf("Nanobenchmark check failed at line %d\n", __LINE__); \
    abort();                                                     \
  }

#if NANOBENCHMARK_ENABLE_CHECKS
#define NANOBENCHMARK_CHECK(condition) NANOBENCHMARK_CHECK_ALWAYS(condition)
#else
#define NANOBENCHMARK_CHECK(condition)
#endif

#if HH_MSC_VERSION

// MSVC does not support inline assembly anymore (and never supported GCC's
// RTL constraints used below).
#pragma optimize("", off)
// Self-assignment with #pragma optimize("off") might be expected to prevent
// elision, but it does not with MSVC 2015.
void UseCharPointer(volatile const char*) {}
#pragma optimize("", on)

template <class T>
inline void PreventElision(T&& output) {
  UseCharPointer(reinterpret_cast<volatile const char*>(&output));
}

#else

// Prevents the compiler from eliding the computations that led to "output".
// Works by indicating to the compiler that "output" is being read and modified.
// The +r constraint avoids unnecessary writes to memory, but only works for
// FuncOutput.
template <class T>
inline void PreventElision(T&& output) {
  asm volatile("" : "+r"(output) : : "memory");
}

#endif

HH_NOINLINE FuncOutput Func1(const FuncInput input) { return input + 1; }
HH_NOINLINE FuncOutput Func2(const FuncInput input) { return input + 2; }

// Cycles elapsed = difference between two cycle counts. Must be unsigned to
// ensure wraparound on overflow.
using Duration = uint32_t;

// Even with high-priority pinned threads and frequency throttling disabled,
// elapsed times are noisy due to interrupts or SMM operations. It might help
// to detect such events via transactions and omit affected measurements.
// Unfortunately, TSX is currently unavailable due to a bug. We achieve
// repeatable results with a robust measure of the central tendency ("mode").

// Returns time elapsed between timer Start/Stop.
Duration EstimateResolutionOnCurrentCPU(const Func func) {
  // Even 128K samples are not enough to achieve repeatable results when
  // throttling is enabled; the caller must perform additional aggregation.
  const size_t kNumSamples = 512;
  Duration samples[kNumSamples];
  for (size_t i = 0; i < kNumSamples; ++i) {
    const volatile Duration t0 = Start<Duration>();
    PreventElision(func(i));
    const volatile Duration t1 = Stop<Duration>();
    NANOBENCHMARK_CHECK(t0 <= t1);
    samples[i] = t1 - t0;
  }
  CountingSort(samples, samples + kNumSamples);
  const Duration resolution = Mode(samples, kNumSamples);
  NANOBENCHMARK_CHECK(resolution != 0);
  return resolution;
}

// Returns mode of EstimateResolutionOnCurrentCPU across all CPUs. This
// increases repeatability because some CPUs may be throttled or slowed down by
// interrupts.
Duration EstimateResolution(const Func func_to_measure) {
  Func func = (func_to_measure == &Func2) ? &Func1 : &Func2;

  const size_t kNumSamples = 512;
  std::vector<Duration> resolutions;
  resolutions.reserve(kNumSamples);

  const auto cpus = AvailableCPUs();
  const size_t repetitions_per_cpu = kNumSamples / cpus.size();

  auto affinity = GetThreadAffinity();
  for (const int cpu : cpus) {
    PinThreadToCPU(cpu);
    for (size_t i = 0; i < repetitions_per_cpu; ++i) {
      resolutions.push_back(EstimateResolutionOnCurrentCPU(func));
    }
  }
  SetThreadAffinity(affinity);
  free(affinity);

  Duration* const begin = resolutions.data();
  CountingSort(begin, begin + resolutions.size());
  const Duration resolution = Mode(begin, resolutions.size());
  printf("Resolution %lu\n", long(resolution));
  return resolution;
}

// Returns cycles elapsed when running an empty region, i.e. the timer
// resolution/overhead, which will be deducted from other measurements and
// also used by InitReplicas.
Duration Resolution(const Func func) {
  // Initialization is expensive and should only happen once.
  static const Duration resolution = EstimateResolution(func);
  return resolution;
}

// Returns cycles elapsed when passing each of "inputs" (after in-place
// shuffling) to "func", which must return something it has computed
// so the compiler does not optimize it away.
Duration CyclesElapsed(const Duration resolution, const Func func,
                       std::vector<FuncInput>* inputs) {
  // This benchmark attempts to measure the performance of "func" when
  // called with realistic inputs, which we assume are randomly drawn
  // from the given "inputs" distribution, so we shuffle those values.
  std::random_shuffle(inputs->begin(), inputs->end());

  const Duration t0 = Start<Duration>();
  for (const FuncInput input : *inputs) {
    PreventElision(func(input));
  }
  const Duration t1 = Stop<Duration>();
  const Duration elapsed = t1 - t0;
  NANOBENCHMARK_CHECK(elapsed > resolution);
  return elapsed - resolution;
}

// Stores input values for a series of calls to the function to measure.
// We assume inputs are drawn from a known discrete probability distribution,
// modeled as a vector<FuncInput> v. The probability of a value X
// in v is count(v.begin(), v.end(), X) / v.size().
class Inputs {
  Inputs(const Inputs&) = delete;
  Inputs& operator=(const Inputs&) = delete;

 public:
  Inputs(const Duration resolution, const std::vector<FuncInput>& distribution,
         const Func func)
      : unique_(InitUnique(distribution)),
        replicas_(InitReplicas(distribution, resolution, func)),
        num_replicas_(replicas_.size() / distribution.size()) {
    printf("NumReplicas %zu\n", num_replicas_);
  }

  // Returns vector of the unique values from the input distribution.
  const std::vector<FuncInput>& Unique() const { return unique_; }

  // Returns how many instances of "distribution" are in "replicas_", i.e.
  // the number of occurrences of an input value that occurred only once
  // in the distribution. This is the divisor for computing the duration
  // of a single call.
  size_t NumReplicas() const { return num_replicas_; }

  // Returns the (replicated) input distribution. Modified by caller
  // (shuffled in-place) => not thread-safe.
  std::vector<FuncInput>& Replicas() { return replicas_; }

  // Returns a copy of Replicas() with NumReplicas() occurrences of "input"
  // removed. Used for the leave-one-out measurement.
  std::vector<FuncInput> Without(const FuncInput input_to_remove) const {
    // "input_to_remove" should be in the original distribution.
    NANOBENCHMARK_CHECK(std::find(unique_.begin(), unique_.end(),
                                  input_to_remove) != unique_.end());

    std::vector<FuncInput> copy = replicas_;
    auto pos = std::partition(copy.begin(), copy.end(),
                              [input_to_remove](const FuncInput input) {
                                return input_to_remove != input;
                              });
    // Must occur at least num_replicas_ times.
    NANOBENCHMARK_CHECK(copy.end() - pos >= num_replicas_);
    // (Avoids unused-variable warning.)
    PreventElision(&*pos);
    copy.resize(copy.size() - num_replicas_);
    return copy;
  }

 private:
  // Returns a copy with any duplicate values removed. Initializing unique_
  // through this function allows it to be const.
  static std::vector<FuncInput> InitUnique(
      const std::vector<FuncInput>& distribution) {
    std::vector<FuncInput> unique = distribution;
    std::sort(unique.begin(), unique.end());
    unique.erase(std::unique(unique.begin(), unique.end()), unique.end());
    // Our leave-one-out measurement technique only makes sense when
    // there are multiple input values.
    NANOBENCHMARK_CHECK(unique.size() >= 2);
    return unique;
  }

  // Returns how many replicas of "distribution" are required before
  // CyclesElapsed is large enough compared to the timer resolution.
  static std::vector<FuncInput> InitReplicas(
      const std::vector<FuncInput>& distribution, const Duration resolution,
      const Func func) {
    // We compute the difference in duration for inputs = Replicas() vs.
    // Without(). Dividing this by num_replicas must yield a value where the
    // quantization error (from the timer resolution) is sufficiently small.
    const uint64_t min_elapsed = distribution.size() * resolution * 400;

    std::vector<FuncInput> replicas;
    for (;;) {
      AppendReplica(distribution, &replicas);

#if NANOBENCHMARK_ENABLE_CHECKS
      const uint64_t t0 = Start64();
#endif
      const Duration elapsed = CyclesElapsed(resolution, func, &replicas);
#if NANOBENCHMARK_ENABLE_CHECKS
      const uint64_t t1 = Stop64();
#endif
      // Ensure the 32-bit timer didn't and won't overflow.
      NANOBENCHMARK_CHECK((t1 - t0) < (1ULL << 30));

      if (elapsed >= min_elapsed) {
        return replicas;
      }
    }
  }

  // Appends all values in "distribution" to "replicas".
  static void AppendReplica(const std::vector<FuncInput>& distribution,
                            std::vector<FuncInput>* replicas) {
    replicas->reserve(replicas->size() + distribution.size());
    for (const FuncInput input : distribution) {
      replicas->push_back(input);
    }
  }

  const std::vector<FuncInput> unique_;

  // Modified by caller (shuffled in-place) => non-const.
  std::vector<FuncInput> replicas_;

  // Initialized from replicas_.
  const size_t num_replicas_;
};

// Holds samples of measured durations, and (robustly) reduces them to a
// single result for each unique input value.
class DurationSamples {
 public:
  DurationSamples(const std::vector<FuncInput>& unique_inputs,
                  const size_t num_samples)
      : num_samples_(num_samples) {
    // Preallocate storage.
    for (const FuncInput input : unique_inputs) {
      samples_for_input_[input].reserve(num_samples);
    }
  }

  void Add(const FuncInput input, const Duration sample) {
    // "input" should be one of the values passed to the ctor.
    NANOBENCHMARK_CHECK(samples_for_input_.find(input) !=
                        samples_for_input_.end());

    samples_for_input_[input].push_back(sample);
  }

  // Invokes "lambda" for each (input, duration) pair. The per-call duration
  // is the central tendency (the mode) of the samples.
  template <class Lambda>
  void Reduce(const Lambda& lambda) {
    for (auto& input_and_samples : samples_for_input_) {
      const FuncInput input = input_and_samples.first;
      std::vector<Duration>& samples = input_and_samples.second;

      NANOBENCHMARK_CHECK(samples.size() <= num_samples_);
      std::sort(samples.begin(), samples.end());
      const Duration duration = Mode(samples.data(), samples.size());
      lambda(input, duration);
    }
  }

 private:
  const size_t num_samples_;
  std::map<FuncInput, std::vector<Duration>> samples_for_input_;
};

// Gathers "num_samples" durations via repeated leave-one-out measurements.
DurationSamples GatherDurationSamples(const Duration resolution, Inputs& inputs,
                                      const Func func,
                                      const size_t num_samples) {
  DurationSamples samples(inputs.Unique(), num_samples);
  for (size_t i = 0; i < num_samples; ++i) {
    // Total duration for all shuffled input values. This may change over time,
    // so recompute it for each sample.
    const Duration total = CyclesElapsed(resolution, func, &inputs.Replicas());

    for (const FuncInput input : inputs.Unique()) {
      // To isolate the durations of the calls with this input value,
      // we measure the duration without those values and subtract that
      // from the total, and later divide by NumReplicas.
      std::vector<FuncInput> without = inputs.Without(input);
      for (int rep = 0; rep < 3; ++rep) {
        const Duration elapsed = CyclesElapsed(resolution, func, &without);
        if (elapsed < total) {
          samples.Add(input, total - elapsed);
          break;
        }
      }
    }
  }
  return samples;
}

}  // namespace

DurationsForInputs::DurationsForInputs(const FuncInput* inputs,
                                       const size_t num_inputs,
                                       const size_t max_durations)
    : num_items(0),
      inputs_(inputs),
      num_inputs_(num_inputs),
      max_durations_(max_durations),
      all_durations_(new float[num_inputs * max_durations]) {
  NANOBENCHMARK_CHECK(num_inputs != 0);
  NANOBENCHMARK_CHECK(max_durations != 0);

  items = new Item[num_inputs];
  for (size_t i = 0; i < num_inputs_; ++i) {
    items[i].input = 0;  // initialized later
    items[i].num_durations = 0;
    items[i].durations = all_durations_ + i * max_durations;
  }
}

DurationsForInputs::~DurationsForInputs() {
  delete[] all_durations_;
  delete[] items;
}

void DurationsForInputs::AddItem(const FuncInput input, const float sample) {
  for (size_t i = 0; i < num_items; ++i) {
    NANOBENCHMARK_CHECK(items[i].input != input);
  }
  Item& item = items[num_items];
  item.input = input;
  item.num_durations = 1;
  item.durations[0] = sample;
  ++num_items;
}

void DurationsForInputs::AddSample(const FuncInput input, const float sample) {
  for (size_t i = 0; i < num_items; ++i) {
    Item& item = items[i];
    if (item.input == input) {
      item.durations[item.num_durations] = sample;
      ++item.num_durations;
      return;
    }
  }
  NANOBENCHMARK_CHECK(!"Item not found");
}

void DurationsForInputs::Item::PrintMedianAndVariability() {
  // Copy so that Median can modify.
  std::vector<float> duration_vec(durations, durations + num_durations);
  const float median = Median(&duration_vec);
  const float variability = MedianAbsoluteDeviation(duration_vec, median);
  printf("%5zu: median=%5.1f cycles; median abs. deviation=%4.1f cycles\n",
         input, median, variability);
}

void MeasureDurations(const Func func, DurationsForInputs* input_map) {
  const Duration resolution = Resolution(func);

  // Adds enough 'replicas' of the distribution to measure "func" given
  // the timer resolution.
  const std::vector<FuncInput> distribution(
      input_map->inputs_, input_map->inputs_ + input_map->num_inputs_);
  Inputs inputs(resolution, distribution, func);
  const double per_call = 1.0 / static_cast<int>(inputs.NumReplicas());

  // First iteration: populate input_map items.
  auto samples = GatherDurationSamples(resolution, inputs, func, 512);
  samples.Reduce(
      [per_call, input_map](const FuncInput input, const Duration duration) {
        const float sample = static_cast<float>(duration * per_call);
        input_map->AddItem(input, sample);
      });

  // Subsequent iteration(s): append to input_map items' array.
  for (size_t rep = 1; rep < input_map->max_durations_; ++rep) {
    auto samples = GatherDurationSamples(resolution, inputs, func, 512);
    samples.Reduce(
        [per_call, input_map](const FuncInput input, const Duration duration) {
          const float sample = static_cast<float>(duration * per_call);
          input_map->AddSample(input, sample);
        });
  }
}

}  // namespace highwayhash

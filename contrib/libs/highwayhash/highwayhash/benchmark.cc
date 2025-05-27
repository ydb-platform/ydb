// Copyright 2016 Google Inc. All Rights Reserved.
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

// Measures hash function throughput for various input sizes.

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "highwayhash/compiler_specific.h"
#include "highwayhash/instruction_sets.h"
#include "highwayhash/nanobenchmark.h"
#include "highwayhash/os_specific.h"
#include "highwayhash/robust_statistics.h"

// Which functions to enable (includes check for compiler support)
#define BENCHMARK_SIP 0
#define BENCHMARK_SIP_TREE 0
#define BENCHMARK_HIGHWAY 1
#define BENCHMARK_HIGHWAY_CAT 1
#define BENCHMARK_FARM 0

#include "highwayhash/highwayhash_test_target.h"
#if BENCHMARK_SIP
#include "highwayhash/sip_hash.h"
#endif
#if BENCHMARK_SIP_TREE
#include "highwayhash/scalar_sip_tree_hash.h"
#include "highwayhash/sip_tree_hash.h"
#endif
#if BENCHMARK_FARM
#include "third_party/farmhash/src/farmhash.h"
#endif

namespace highwayhash {
namespace {

// Stores time measurements from benchmarks, with support for printing them
// as LaTeX figures or tables.
class Measurements {
 public:
  void Add(const char* caption, const size_t bytes, const double cycles) {
    const float cpb = static_cast<float>(cycles / bytes);
    results_.emplace_back(caption, static_cast<int>(bytes), cpb);
  }

  // Prints results as a LaTeX table (only for in_sizes matching the
  // desired values).
  void PrintTable(const std::vector<size_t>& in_sizes) {
    std::vector<size_t> unique = in_sizes;
    std::sort(unique.begin(), unique.end());
    unique.erase(std::unique(unique.begin(), unique.end()), unique.end());

    printf("\\begin{tabular}{");
    for (size_t i = 0; i < unique.size() + 1; ++i) {
      printf("%s", i == 0 ? "r" : "|r");
    }
    printf("}\n\\toprule\nAlgorithm");
    for (const size_t in_size : unique) {
      printf(" & %zu", in_size);
    }
    printf("\\\\\n\\midrule\n");

    const SpeedsForCaption cpb_for_caption = SortByCaptionFilterBySize(unique);
    for (const auto& item : cpb_for_caption) {
      printf("%22s", item.first.c_str());
      for (const float cpb : item.second) {
        printf(" & %5.2f", cpb);
      }
      printf("\\\\\n");
    }
  }

  // Prints results suitable for pgfplots.
  void PrintPlots() {
    const SpeedsForCaption cpb_for_caption = SortByCaption();
    assert(!cpb_for_caption.empty());
    const size_t num_sizes = cpb_for_caption.begin()->second.size();

    printf("Size ");
    // Flatten per-caption vectors into one iterator.
    std::vector<std::vector<float>::const_iterator> iterators;
    for (const auto& item : cpb_for_caption) {
      printf("%21s ", item.first.c_str());
      assert(item.second.size() == num_sizes);
      iterators.push_back(item.second.begin());
    }
    printf("\n");

    const std::vector<int>& sizes = UniqueSizes();
    assert(num_sizes == sizes.size());
    for (int i = 0; i < static_cast<int>(num_sizes); ++i) {
      printf("%d ", sizes[i]);
      for (auto& it : iterators) {
        printf("%5.2f ", 1.0f / *it);  // bytes per cycle
        ++it;
      }
      printf("\n");
    }
  }

 private:
  struct Result {
    Result(const char* caption, const int in_size, const float cpb)
        : caption(caption), in_size(in_size), cpb(cpb) {}

    // Algorithm name.
    std::string caption;
    // Size of the input data [bytes].
    int in_size;
    // Measured throughput [cycles per byte].
    float cpb;
  };

  // Returns set of all input sizes for the first column of a size/speed plot.
  std::vector<int> UniqueSizes() {
    std::vector<int> sizes;
    sizes.reserve(results_.size());
    for (const Result& result : results_) {
      sizes.push_back(result.in_size);
    }
    std::sort(sizes.begin(), sizes.end());
    sizes.erase(std::unique(sizes.begin(), sizes.end()), sizes.end());
    return sizes;
  }

  using SpeedsForCaption = std::map<std::string, std::vector<float>>;

  SpeedsForCaption SortByCaption() const {
    SpeedsForCaption cpb_for_caption;
    for (const Result& result : results_) {
      cpb_for_caption[result.caption].push_back(result.cpb);
    }
    return cpb_for_caption;
  }

  // Only includes measurement results matching one of the given sizes.
  SpeedsForCaption SortByCaptionFilterBySize(
      const std::vector<size_t>& in_sizes) const {
    SpeedsForCaption cpb_for_caption;
    for (const Result& result : results_) {
      for (const size_t in_size : in_sizes) {
        if (result.in_size == static_cast<int>(in_size)) {
          cpb_for_caption[result.caption].push_back(result.cpb);
        }
      }
    }
    return cpb_for_caption;
  }

  std::vector<Result> results_;
};

void AddMeasurements(DurationsForInputs* input_map, const char* caption,
                     Measurements* measurements) {
  for (size_t i = 0; i < input_map->num_items; ++i) {
    const DurationsForInputs::Item& item = input_map->items[i];
    std::vector<float> durations(item.durations,
                                 item.durations + item.num_durations);
    const float median = Median(&durations);
    const float variability = MedianAbsoluteDeviation(durations, median);
    printf("%s %4zu: median=%6.1f cycles; median L1 norm =%4.1f cycles\n",
           caption, item.input, median, variability);
    measurements->Add(caption, item.input, median);
  }
  input_map->num_items = 0;
}

#if BENCHMARK_SIP || BENCHMARK_FARM || (BENCHMARK_SIP_TREE && defined(__AVX2__))

void MeasureAndAdd(DurationsForInputs* input_map, const char* caption,
                   const Func func, Measurements* measurements) {
  MeasureDurations(func, input_map);
  AddMeasurements(input_map, caption, measurements);
}

#endif

// InstructionSets::RunAll callback.
void AddMeasurementsWithPrefix(const char* prefix, const char* target_name,
                               DurationsForInputs* input_map, void* context) {
  std::string caption(prefix);
  caption += target_name;
  AddMeasurements(input_map, caption.c_str(),
                  static_cast<Measurements*>(context));
}

#if BENCHMARK_SIP

uint64_t RunSip(const size_t size) {
  const HH_U64 key2[2] HH_ALIGNAS(16) = {0, 1};
  char in[kMaxBenchmarkInputSize];
  memcpy(in, &size, sizeof(size));
  return SipHash(key2, in, size);
}

uint64_t RunSip13(const size_t size) {
  const HH_U64 key2[2] HH_ALIGNAS(16) = {0, 1};
  char in[kMaxBenchmarkInputSize];
  memcpy(in, &size, sizeof(size));
  return SipHash13(key2, in, size);
}

#endif

#if BENCHMARK_SIP_TREE

uint64_t RunSipTree(const size_t size) {
  const HH_U64 key4[4] HH_ALIGNAS(32) = {0, 1, 2, 3};
  char in[kMaxBenchmarkInputSize];
  memcpy(in, &size, sizeof(size));
  return SipTreeHash(key4, in, size);
}

uint64_t RunSipTree13(const size_t size) {
  const HH_U64 key4[4] HH_ALIGNAS(32) = {0, 1, 2, 3};
  char in[kMaxBenchmarkInputSize];
  memcpy(in, &size, sizeof(size));
  return SipTreeHash13(key4, in, size);
}

#endif

#if BENCHMARK_FARM

uint64_t RunFarm(const size_t size) {
  char in[kMaxBenchmarkInputSize];
  memcpy(in, &size, sizeof(size));
  return farmhash::Fingerprint64(reinterpret_cast<const char*>(in), size);
}

#endif

void AddMeasurements(const std::vector<size_t>& in_sizes,
                     Measurements* measurements) {
  DurationsForInputs input_map(in_sizes.data(), in_sizes.size(), 40);
#if BENCHMARK_SIP
  MeasureAndAdd(&input_map, "SipHash", RunSip, measurements);
  MeasureAndAdd(&input_map, "SipHash13", RunSip13, measurements);
#endif

#if BENCHMARK_SIP_TREE && defined(__AVX2__)
  MeasureAndAdd(&input_map, "SipTreeHash", RunSipTree, measurements);
  MeasureAndAdd(&input_map, "SipTreeHash13", RunSipTree13, measurements);
#endif

#if BENCHMARK_FARM
  MeasureAndAdd(&input_map, "Farm", &RunFarm, measurements);
#endif

#if BENCHMARK_HIGHWAY
  InstructionSets::RunAll<HighwayHashBenchmark>(
      &input_map, &AddMeasurementsWithPrefix, measurements);
#endif

#if BENCHMARK_HIGHWAY_CAT
  InstructionSets::RunAll<HighwayHashCatBenchmark>(
      &input_map, &AddMeasurementsWithPrefix, measurements);
#endif
}

void PrintTable() {
  const std::vector<size_t> in_sizes = {
      7, 8, 31, 32, 63, 64, kMaxBenchmarkInputSize};
  Measurements measurements;
  AddMeasurements(in_sizes, &measurements);
  measurements.PrintTable(in_sizes);
}

void PrintPlots() {
  std::vector<size_t> in_sizes;
  for (int num_vectors = 0; num_vectors < 12; ++num_vectors) {
    for (int remainder : {0, 9, 18, 27}) {
      in_sizes.push_back(num_vectors * 32 + remainder);
      assert(in_sizes.back() <= kMaxBenchmarkInputSize);
    }
  }

  Measurements measurements;
  AddMeasurements(in_sizes, &measurements);
  measurements.PrintPlots();
}

}  // namespace
}  // namespace highwayhash

int main(int argc, char* argv[]) {
  highwayhash::PinThreadToRandomCPU();
  // No argument or t => table
  if (argc < 2 || argv[1][0] == 't') {
    highwayhash::PrintTable();
  } else if (argv[1][0] == 'p') {
    highwayhash::PrintPlots();
  }
  return 0;
}

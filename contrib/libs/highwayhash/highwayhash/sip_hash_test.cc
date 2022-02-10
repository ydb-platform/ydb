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

#include "highwayhash/sip_hash.h"

#include <cassert>
#include <numeric>
#include <stdio.h>
#include <stdlib.h>

#ifdef HH_GOOGLETEST
#include "base/integral_types.h"
#include "testing/base/public/benchmark.h"
#include "testing/base/public/gunit.h"
#endif
#include "highwayhash/scalar_sip_tree_hash.h"
#include "highwayhash/sip_tree_hash.h"

namespace highwayhash {
namespace {

void VerifySipHash() {
  const int kMaxSize = 64;
  char in[kMaxSize];  // empty string, 00, 00 01, ...
  const HH_U64 key[2] = {0x0706050403020100ULL, 0x0F0E0D0C0B0A0908ULL};

  // Known-good SipHash-2-4 output from D. Bernstein.
  const HH_U64 kSipHashOutput[64] = {
      0x726FDB47DD0E0E31, 0x74F839C593DC67FD, 0x0D6C8009D9A94F5A,
      0x85676696D7FB7E2D, 0xCF2794E0277187B7, 0x18765564CD99A68D,
      0xCBC9466E58FEE3CE, 0xAB0200F58B01D137, 0x93F5F5799A932462,
      0x9E0082DF0BA9E4B0, 0x7A5DBBC594DDB9F3, 0xF4B32F46226BADA7,
      0x751E8FBC860EE5FB, 0x14EA5627C0843D90, 0xF723CA908E7AF2EE,
      0xA129CA6149BE45E5, 0x3F2ACC7F57C29BDB, 0x699AE9F52CBE4794,
      0x4BC1B3F0968DD39C, 0xBB6DC91DA77961BD, 0xBED65CF21AA2EE98,
      0xD0F2CBB02E3B67C7, 0x93536795E3A33E88, 0xA80C038CCD5CCEC8,
      0xB8AD50C6F649AF94, 0xBCE192DE8A85B8EA, 0x17D835B85BBB15F3,
      0x2F2E6163076BCFAD, 0xDE4DAAACA71DC9A5, 0xA6A2506687956571,
      0xAD87A3535C49EF28, 0x32D892FAD841C342, 0x7127512F72F27CCE,
      0xA7F32346F95978E3, 0x12E0B01ABB051238, 0x15E034D40FA197AE,
      0x314DFFBE0815A3B4, 0x027990F029623981, 0xCADCD4E59EF40C4D,
      0x9ABFD8766A33735C, 0x0E3EA96B5304A7D0, 0xAD0C42D6FC585992,
      0x187306C89BC215A9, 0xD4A60ABCF3792B95, 0xF935451DE4F21DF2,
      0xA9538F0419755787, 0xDB9ACDDFF56CA510, 0xD06C98CD5C0975EB,
      0xE612A3CB9ECBA951, 0xC766E62CFCADAF96, 0xEE64435A9752FE72,
      0xA192D576B245165A, 0x0A8787BF8ECB74B2, 0x81B3E73D20B49B6F,
      0x7FA8220BA3B2ECEA, 0x245731C13CA42499, 0xB78DBFAF3A8D83BD,
      0xEA1AD565322A1A0B, 0x60E61C23A3795013, 0x6606D7E446282B93,
      0x6CA4ECB15C5F91E1, 0x9F626DA15C9625F3, 0xE51B38608EF25F57,
      0x958A324CEB064572};

  for (int size = 0; size < kMaxSize; ++size) {
    in[size] = static_cast<char>(size);
    const HH_U64 hash = highwayhash::SipHash(key, in, size);
#ifdef HH_GOOGLETEST
    EXPECT_EQ(kSipHashOutput[size], hash) << "Mismatch at length " << size;
#else
    if (hash != kSipHashOutput[size]) {
      printf("Mismatch at length %d\n", size);
      abort();
    }
#endif
  }
}

#ifdef HH_GOOGLETEST
TEST(SipHashTest, OutputMatchesExpectations) { VerifySipHash(); }

namespace bm {
/* Run with:
   blaze run -c opt --cpu=haswell third_party/highwayhash:sip_hash_test -- \
     --benchmarks=all --benchmark_min_iters=1 --benchmark_min_time=0.25
*/

// Returns a pointer to memory of at least size bytes long to be used as hashing
// input.
char* GetInput(size_t size) {
  static constexpr size_t kMaxSize = 100 << 20;
  assert(size <= kMaxSize);
  static auto* res = []() {
    auto* res = new char[kMaxSize];
    std::iota(res, res + kMaxSize, 0);
    return res;
  }();
  return res;
}

template <class Hasher>
void BM(int iters, int size) {
  StopBenchmarkTiming();
  auto* input = GetInput(size);
  const HH_U64 keys[4] = {0x0706050403020100ULL, 0x0F0E0D0C0B0A0908ULL,
                          0x1716151413121110ULL, 0x1F1E1D1C1B1A1918ULL};
  Hasher hasher(keys);
  StartBenchmarkTiming();
  for (int i = 0; i < iters; ++i) {
    testing::DoNotOptimize(hasher(input, size));
  }
  StopBenchmarkTiming();
  SetBenchmarkBytesProcessed(static_cast<int64>(iters) * size);
}

void Args(::testing::Benchmark* bm) {
  bm->DenseRange(1, 16)->Range(32, 100 << 20);
}

#define DEFINE_HASHER(hashfn, num_keys)                            \
  struct hashfn##er {                                              \
    hashfn##er(const HH_U64* k) { memcpy(keys, k, sizeof(keys)); } \
    HH_U64 operator()(const char* input, size_t size) {            \
      return highwayhash::hashfn(keys, input, size);               \
    }                                                              \
    HH_U64 keys[num_keys];                                         \
  }

DEFINE_HASHER(SipHash, 2);
BENCHMARK(BM<SipHasher>)->Apply(Args);

DEFINE_HASHER(ScalarSipTreeHash, 4);
BENCHMARK(BM<ScalarSipTreeHasher>)->Apply(Args);

#ifdef __AVX2__
DEFINE_HASHER(SipTreeHash, 4);
BENCHMARK(BM<SipTreeHasher>)->Apply(Args);
#endif

}  // namespace bm
#endif  // HH_GOOGLETEST

}  // namespace
}  // namespace highwayhash

#ifndef HH_GOOGLETEST
int main(int argc, char* argv[]) {
  highwayhash::VerifySipHash();
  printf("VerifySipHash succeeded.\n");
  return 0;
}
#endif

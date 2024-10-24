// Minimal usage example: prints a hash. Tested on x86, ppc, arm.

#include "highwayhash/highwayhash.h"

#include <algorithm>
#include <iostream>

using namespace highwayhash;

int main(int argc, char* argv[]) {
  // Please use a different key to ensure your hashes aren't identical.
  const HHKey key HH_ALIGNAS(32) = {1, 2, 3, 4};
  // Aligning inputs to 32 bytes may help but is not required.
  const char in[] = "bytes_to_hash";
  // Type determines the hash size; can also be HHResult128 or HHResult256.
  HHResult64 result;
  // HH_TARGET_PREFERRED expands to the best specialization available for the
  // CPU detected via compiler flags (e.g. AVX2 #ifdef __AVX2__).
  HHStateT<HH_TARGET_PREFERRED> state(key);
  // Using argc prevents the compiler from eliding the hash computations.
  const size_t size = std::min(sizeof(in), static_cast<size_t>(argc));
  HighwayHashT(&state, in, size, &result);
  std::cout << "Hash   : " << result << std::endl;

  HighwayHashCatT<HH_TARGET_PREFERRED> cat(key);
  cat.Append(in, size);
  cat.Finalize(&result);
  std::cout << "HashCat: " << result << std::endl;
  return 0;
}

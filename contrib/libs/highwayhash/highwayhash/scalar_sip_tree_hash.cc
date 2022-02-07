// Copyright 2015 Google Inc. All Rights Reserved.
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

#include "highwayhash/scalar_sip_tree_hash.h"

#include <cstddef>
#include <cstring>  // memcpy

#include "highwayhash/compiler_specific.h"
#include "highwayhash/sip_hash.h"

namespace highwayhash {
namespace {

// Paper: https://www.131002.net/siphash/siphash.pdf
// SSE41 implementation: https://goo.gl/80GBSD
// Tree hash extension: http://dx.doi.org/10.4236/jis.2014.53010

// The hash state is updated by injecting 4x8-byte packets;
// XORing together all state vectors yields 32 bytes that are
// reduced to 64 bits via 8-byte SipHash.

const int kNumLanes = 4;
using Lanes = HH_U64[kNumLanes];
const int kPacketSize = sizeof(Lanes);

template <int kUpdateRounds, int kFinalizeRounds>
class ScalarSipTreeHashState {
 public:
  HH_INLINE ScalarSipTreeHashState(const Lanes& keys, const int lane) {
    const HH_U64 key = keys[lane] ^ (kNumLanes | lane);
    v0 = 0x736f6d6570736575ull ^ key;
    v1 = 0x646f72616e646f6dull ^ key;
    v2 = 0x6c7967656e657261ull ^ key;
    v3 = 0x7465646279746573ull ^ key;
  }

  HH_INLINE void Update(const HH_U64& packet) {
    v3 ^= packet;

    Compress<kUpdateRounds>();

    v0 ^= packet;
  }

  HH_INLINE HH_U64 Finalize() {
    // Mix in bits to avoid leaking the key if all packets were zero.
    v2 ^= 0xFF;

    Compress<kFinalizeRounds>();

    return (v0 ^ v1) ^ (v2 ^ v3);
  }

 private:
  // Rotate a 64-bit value "v" left by N bits.
  template <HH_U64 bits>
  static HH_INLINE HH_U64 RotateLeft(const HH_U64 v) {
    const HH_U64 left = v << bits;
    const HH_U64 right = v >> (64 - bits);
    return left | right;
  }

  template <int kRounds>
  HH_INLINE void Compress() {
    for (int i = 0; i < kRounds; ++i) {
      // ARX network: add, rotate, exclusive-or.
      v0 += v1;
      v2 += v3;
      v1 = RotateLeft<13>(v1);
      v3 = RotateLeft<16>(v3);
      v1 ^= v0;
      v3 ^= v2;

      v0 = RotateLeft<32>(v0);

      v2 += v1;
      v0 += v3;
      v1 = RotateLeft<17>(v1);
      v3 = RotateLeft<21>(v3);
      v1 ^= v2;
      v3 ^= v0;

      v2 = RotateLeft<32>(v2);
    }
  }

  HH_U64 v0;
  HH_U64 v1;
  HH_U64 v2;
  HH_U64 v3;
};

}  // namespace

template <size_t kUpdateRounds, size_t kFinalizeRounds>
HH_U64 ScalarSipTreeHashT(const Lanes& key, const char* bytes,
                          const HH_U64 size) {
  // "j-lanes" tree hashing interleaves 8-byte input packets.
  using State = ScalarSipTreeHashState<kUpdateRounds, kFinalizeRounds>;
  State state[kNumLanes] = {State(key, 0), State(key, 1), State(key, 2),
                            State(key, 3)};

  // Hash entire 32-byte packets.
  const size_t remainder = size & (kPacketSize - 1);
  const size_t truncated_size = size - remainder;
  const HH_U64* packets = reinterpret_cast<const HH_U64*>(bytes);
  for (size_t i = 0; i < truncated_size / kPacketSize; ++i) {
    for (int lane = 0; lane < kNumLanes; ++lane) {
      const HH_U64 packet = *packets++;
      state[lane].Update(packet);
    }
  }

  // Update with final 32-byte packet.
  const size_t remainder_mod4 = remainder & 3;
  uint32_t packet4 = static_cast<uint32_t>(remainder << 24);
  const char* final_bytes = bytes + size - remainder_mod4;
  for (size_t i = 0; i < remainder_mod4; ++i) {
    const uint32_t byte = static_cast<unsigned char>(final_bytes[i]);
    packet4 += byte << (i * 8);
  }

  char final_packet[kPacketSize] = {0};
  memcpy(final_packet, bytes + truncated_size, remainder - remainder_mod4);
  memcpy(final_packet + kPacketSize - 4, &packet4, sizeof(packet4));
  packets = reinterpret_cast<const HH_U64*>(final_packet);
  for (int lane = 0; lane < kNumLanes; ++lane) {
    state[lane].Update(packets[lane]);
  }

  // Store the resulting hashes.
  uint64_t hashes[4];
  for (int lane = 0; lane < kNumLanes; ++lane) {
    hashes[lane] = state[lane].Finalize();
  }

  typename SipHashStateT<kUpdateRounds, kFinalizeRounds>::Key reduce_key;
  memcpy(&reduce_key, &key, sizeof(reduce_key));
  return ReduceSipTreeHash<kNumLanes, kUpdateRounds, kFinalizeRounds>(
      reduce_key, hashes);
}

HH_U64 ScalarSipTreeHash(const Lanes& key, const char* bytes,
                         const HH_U64 size) {
  return ScalarSipTreeHashT<2, 4>(key, bytes, size);
}

HH_U64 ScalarSipTreeHash13(const Lanes& key, const char* bytes,
                           const HH_U64 size) {
  return ScalarSipTreeHashT<1, 3>(key, bytes, size);
}
}  // namespace highwayhash

using highwayhash::HH_U64;
using highwayhash::ScalarSipTreeHash;
using highwayhash::ScalarSipTreeHash13;
using Key = HH_U64[4];

extern "C" {

HH_U64 ScalarSipTreeHashC(const HH_U64* key, const char* bytes,
                          const HH_U64 size) {
  return ScalarSipTreeHash(*reinterpret_cast<const Key*>(key), bytes, size);
}

HH_U64 ScalarSipTreeHash13C(const HH_U64* key, const char* bytes,
                            const HH_U64 size) {
  return ScalarSipTreeHash13(*reinterpret_cast<const Key*>(key), bytes, size);
}

}  // extern "C"

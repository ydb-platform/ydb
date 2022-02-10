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

#ifndef HIGHWAYHASH_HIGHWAYHASH_H_
#define HIGHWAYHASH_HIGHWAYHASH_H_

// This header's templates are useful for inlining into other CPU-specific code:
// template<TargetBits Target> CodeUsingHash() { HighwayHashT<Target>(...); },
// and can also be instantiated with HH_TARGET when callers don't care about the
// exact implementation. Otherwise, they are implementation details of the
// highwayhash_target wrapper. Use that instead if you need to detect the best
// available implementation at runtime.

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/iaca.h"

// Include exactly one (see arch_specific.h) header, which defines a state
// object in a target-specific namespace, e.g. AVX2::HHStateAVX2.
// Attempts to use "computed includes" (#define MACRO "path/or_just_filename",
// #include MACRO) fail with 'file not found', so we need an #if chain.
#if HH_TARGET == HH_TARGET_AVX2
#include "highwayhash/hh_avx2.h"
#elif HH_TARGET == HH_TARGET_SSE41
#include "highwayhash/hh_sse41.h"
#elif HH_TARGET == HH_TARGET_Portable
#include "highwayhash/hh_portable.h"
#else
#error "Unknown target, add its hh_*.h include here."
#endif

#ifndef HH_DISABLE_TARGET_SPECIFIC
namespace highwayhash {

// Alias templates (HHStateT) cannot be specialized, so we need a helper struct.
// Note that hh_*.h don't just specialize HHStateT directly because vector128.h
// must reside in a distinct namespace (to allow including it from multiple
// translation units), and it is easier if its users, i.e. the concrete HHState,
// also reside in that same namespace, which precludes specialization.
template <TargetBits Target>
struct HHStateForTarget {};

template <>
struct HHStateForTarget<HH_TARGET> {
  // (The namespace is sufficient and the additional HH_TARGET_NAME suffix is
  // technically redundant, but it makes searching easier.)
  using type = HH_TARGET_NAME::HH_ADD_TARGET_SUFFIX(HHState);
};

// Typically used as HHStateT<HH_TARGET>. It would be easier to just have a
// concrete type HH_STATE, but this alias template is required by the
// templates in highwayhash_target.cc.
template <TargetBits Target>
using HHStateT = typename HHStateForTarget<Target>::type;

// Computes HighwayHash of "bytes" using the implementation chosen by "State".
//
// "state" is a HHStateT<> initialized with a key.
// "bytes" is the data to hash (possibly unaligned).
// "size" is the number of bytes to hash; we do not read any additional bytes.
// "hash" is a HHResult* (either 64, 128 or 256 bits).
//
// HighwayHash is a strong pseudorandom function with security claims
// [https://arxiv.org/abs/1612.06257]. It is intended as a safer general-purpose
// hash, about 4x faster than SipHash and 10x faster than BLAKE2.
//
// This template allows callers (e.g. tests) to invoke a specific
// implementation. It must be compiled with the flags required by the desired
// implementation. If the entire program cannot be built with these flags, use
// the wrapper in highwayhash_target.h instead.
//
// Callers wanting to hash multiple pieces of data should duplicate this
// function, calling HHStateT::Update for each input and only Finalizing once.
template <class State, typename Result>
HH_INLINE void HighwayHashT(State* HH_RESTRICT state,
                            const char* HH_RESTRICT bytes, const size_t size,
                            Result* HH_RESTRICT hash) {
  // BeginIACA();
  const size_t remainder = size & (sizeof(HHPacket) - 1);
  const size_t truncated = size & ~(sizeof(HHPacket) - 1);
  for (size_t offset = 0; offset < truncated; offset += sizeof(HHPacket)) {
    state->Update(*reinterpret_cast<const HHPacket*>(bytes + offset));
  }

  if (remainder != 0) {
    state->UpdateRemainder(bytes + truncated, remainder);
  }

  state->Finalize(hash);
  // EndIACA();
}

// Wrapper class for incrementally hashing a series of data ranges. The final
// result is the same as HighwayHashT of the concatenation of all the ranges.
// This is useful for computing the hash of cords, iovecs, and similar
// data structures.
template <TargetBits Target>
class HighwayHashCatT {
 public:
  HH_INLINE HighwayHashCatT(const HHKey& key) : state_(key) {
    // Avoids msan uninitialized-memory warnings.
    HHStateT<Target>::ZeroInitialize(buffer_);
  }

  // Resets the state of the hasher so it can be used to hash a new string.
  HH_INLINE void Reset(const HHKey& key) {
    state_.Reset(key);
    buffer_usage_ = 0;
  }

  // Adds "bytes" to the internal buffer, feeding it to HHStateT::Update as
  // required. Call this as often as desired. Only reads bytes within the
  // interval [bytes, bytes + num_bytes). "num_bytes" == 0 has no effect.
  // There are no alignment requirements.
  HH_INLINE void Append(const char* HH_RESTRICT bytes, size_t num_bytes) {
    // BeginIACA();
    const size_t capacity = sizeof(HHPacket) - buffer_usage_;
    // New bytes fit within buffer, but still not enough to Update.
    if (HH_UNLIKELY(num_bytes < capacity)) {
      HHStateT<Target>::AppendPartial(bytes, num_bytes, buffer_, buffer_usage_);
      buffer_usage_ += num_bytes;
      return;
    }

    // HACK: ensures the state is kept in SIMD registers; otherwise, Update
    // constantly load/stores its operands, which is much slower.
    // Restrict-qualified pointers to external state or the state_ member are
    // not sufficient for keeping this in registers.
    HHStateT<Target> state_copy = state_;

    // Have prior bytes to flush.
    const size_t buffer_usage = buffer_usage_;
    if (HH_LIKELY(buffer_usage != 0)) {
      // Calls update with prior buffer contents plus new data. Does not modify
      // the buffer because some implementations can load into SIMD registers
      // and Append to them directly.
      state_copy.AppendAndUpdate(bytes, capacity, buffer_, buffer_usage);
      bytes += capacity;
      num_bytes -= capacity;
    }

    // Buffer currently empty => Update directly from the source.
    while (num_bytes >= sizeof(HHPacket)) {
      state_copy.Update(*reinterpret_cast<const HHPacket*>(bytes));
      bytes += sizeof(HHPacket);
      num_bytes -= sizeof(HHPacket);
    }

    // Unconditionally assign even if zero because we didn't reset to zero
    // after the AppendAndUpdate above.
    buffer_usage_ = num_bytes;

    state_ = state_copy;

    // Store any remainders in buffer, no-op if multiple of a packet.
    if (HH_LIKELY(num_bytes != 0)) {
      HHStateT<Target>::CopyPartial(bytes, num_bytes, buffer_);
    }
    // EndIACA();
  }

  // Stores the resulting 64, 128 or 256-bit hash of all data passed to Append.
  // Must be called exactly once, or after a prior Reset.
  template <typename Result>  // HHResult*
  HH_INLINE void Finalize(Result* HH_RESTRICT hash) {
    // BeginIACA();
    HHStateT<Target> state_copy = state_;
    const size_t buffer_usage = buffer_usage_;
    if (HH_LIKELY(buffer_usage != 0)) {
      state_copy.UpdateRemainder(buffer_, buffer_usage);
    }
    state_copy.Finalize(hash);
    // EndIACA();
  }

 private:
  HHPacket buffer_ HH_ALIGNAS(64);
  HHStateT<Target> state_ HH_ALIGNAS(32);
  // How many bytes in buffer_ (starting with offset 0) are valid.
  size_t buffer_usage_ = 0;
};

}  // namespace highwayhash
#endif  // HH_DISABLE_TARGET_SPECIFIC
#endif  // HIGHWAYHASH_HIGHWAYHASH_H_

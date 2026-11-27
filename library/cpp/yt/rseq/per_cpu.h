#pragma once

#include <util/system/types.h>

#include <concepts>
#include <cstddef>
#include <type_traits>

namespace NYT::NRseq {

// The library is Linux-only: rseq is a Linux kernel feature and the build enforces OS_LINUX
// (see ya.make); off the rseq fast path the primitives fall back to plain atomics.

////////////////////////////////////////////////////////////////////////////////

//! Number of shards a per-CPU array must provide -- one per CPU.
/*!
 *  Equals nr_cpu_ids (highest possible CPU id + 1), from /sys/devices/system/cpu/possible so
 *  it covers offlined and hot-pluggable CPUs. The fast path indexes by the raw rseq cpu_id, so
 *  a plain count (e.g. _SC_NPROCESSORS_CONF) would undersize the array on sparse topologies --
 *  it is only the fallback when the bitmap is unreadable. Always >= 1; cached.
 */
int GetCpuCount();

//! Adds |delta| to a slot of a per-CPU array of shards, lock-free.
/*!
 *  |shards| is an array of GetCpuCount() |TShard| slots (typically cache-line padded); |field|
 *  selects the |TValue| to update. The field must be 8-byte aligned, and sizeof(TShard) must
 *  be a multiple of 8. Call #GetCpuCount before any updates to initialize the slot count.
 *
 *  Fast path (x86-64 Linux): a non-atomic read-modify-write committed by an rseq critical
 *  section. Otherwise an atomic fetch_add is used. The fast path selects the calling CPU's
 *  slot; the fallback may select any slot in [0, GetCpuCount()). In the exceptional case
 *  where rseq registration succeeds for only some threads, atomic and rseq updates may
 *  collide on a slot and lose counter updates. The same can happen with sparse CPU ids
 *  when the possible-CPU bitmap is unavailable (see per_cpu.cpp).
 *
 *  On x86-64, call only if #IsPerCpuFastPathSupported returns true; even calls that fall back
 *  to atomics read the rseq area at the cached offset.
 *
 *  WARNING (fiber TLS): on x86-64, reach #AddPerCpu only via a
 *  non-inlinable, fiber-switch-free frame (a virtual call or YT_PREVENT_TLS_CACHING; see
 *  library/cpp/yt/misc/tls.h).
 */
template <class TShard, class TValue>
    requires std::integral<TValue> && (sizeof(TValue) == 8)
void AddPerCpu(TShard* shards, TValue TShard::* field, TValue delta);

//! Stores |value| (8 or 16 bytes) into a slot of a per-CPU array of shards, lock-free.
/*!
 *  |shards| / |field| and initialization, alignment and platform requirements
 *  are as in #AddPerCpu; |TValue| is an 8- or 16-byte trivially-copyable type.
 *
 *  Fast path (x86-64 Linux): an rseq-committed store (movq for 8 bytes, movdqu for 16);
 *  otherwise relaxed atomic store(s). Slot selection is the same as in #AddPerCpu.
 *  An 8-byte store is single-copy atomic, never torn; a 16-byte store is not atomic on either
 *  path, so a reader on another CPU may observe a mixture of different stored values.
 *
 *  WARNING (fiber TLS): same contract as #AddPerCpu.
 */
template <class TShard, class TValue>
    requires (sizeof(TValue) == 8 || sizeof(TValue) == 16) && std::is_trivially_copyable_v<TValue>
void StorePerCpu(TShard* shards, TValue TShard::* field, TValue value);

//! Relaxed atomic load of slot |index| -- the reader counterpart of #AddPerCpu.
/*!
 *  |shards| / |field| as in #AddPerCpu; reads shards[index].*field for |index| in
 *  [0, GetCpuCount()). Not tied to the calling CPU (no rseq fast path); aggregate a counter by
 *  loading every slot and summing.
 */
template <class TShard, class TValue>
    requires std::integral<TValue> && (sizeof(TValue) == 8)
TValue LoadPerCpu(const TShard* shards, TValue TShard::* field, int index);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRseq

#define PER_CPU_INL_H_
#include "per_cpu-inl.h"
#undef PER_CPU_INL_H_

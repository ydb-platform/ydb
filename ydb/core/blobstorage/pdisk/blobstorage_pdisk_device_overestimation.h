#pragma once

#include <ydb/library/pdisk_io/device_io_sample.h>
#include <ydb/core/util/hp_timer_helpers.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NKikimr::NPDisk {

// Aggregates raw TDeviceIoSample-s produced by multiple sources that all issue
// I/O to the same physical device (PDisk's own block device thread, DDisk's
// io_uring completion poller, PersistentBuffer's io_uring completion poller),
// merges them into a single completion-ordered stream, and derives the same
// "device overestimation" statistic that TRealBlockDevice::TSharedCallback
// computes for the classic (non-uring) path -- see
// blobstorage_pdisk_blockdevice_async.cpp, TSharedCallback::Exec.
//
// Thread-safety: Push() may be called concurrently from any number of threads
// (each source's own completion thread). ComputeAndReset() is expected to be
// called periodically from a single thread (the owning TPDisk's actor/device
// thread) and is not safe to call concurrently with itself.
class TDeviceOverestimationAggregator {
public:
    struct TWindowResult {
        ui64 EstimatedNs = 0;
        ui64 ActualNs = 0;
        ui64 SampleCount = 0;
    };

    explicit TDeviceOverestimationAggregator(ui64 maxBufferedSamples = 1u << 16)
        : MaxBufferedSamples(maxBufferedSamples)
    {}

    // Called from any producer thread. Cheap: acquires a mutex and appends to
    // a vector. If the buffer is at capacity, the oldest sample is dropped and
    // DroppedSamples is incremented (backpressure without blocking the I/O
    // hot path).
    void Push(const TDeviceIoSample& sample) {
        TGuard<TMutex> guard(Mutex);
        if (Buffered.size() >= MaxBufferedSamples) {
            // Drop oldest half to amortize the cost of frequent single-item drops
            // under sustained overflow, rather than erasing one element at a time.
            const size_t toDrop = Buffered.size() / 2 + 1;
            Buffered.erase(Buffered.begin(), Buffered.begin() + toDrop);
            DroppedSamples += toDrop;
        }
        Buffered.push_back(sample);
    }

    ui64 GetDroppedSamples() const {
        TGuard<TMutex> guard(Mutex);
        return DroppedSamples;
    }

    // Drains all currently buffered samples, merges them by CompleteCycles
    // (across all sources and any prior state carried from previous windows),
    // and applies the parallelism-1 actual-duration model plus the
    // seek-expected heuristic (mirroring TSharedCallback::Exec) to compute
    // this window's estimated/actual cost sums. Must be called from a single
    // thread; carries continuity state (EndOffset/PrevCompleteCycles) across
    // calls the same way the legacy single-threaded computation does.
    TWindowResult ComputeAndReset(ui64 seekCostNs) {
        TVector<TDeviceIoSample> samples;
        {
            TGuard<TMutex> guard(Mutex);
            samples.swap(Buffered);
        }

        TWindowResult result;
        if (samples.empty()) {
            return result;
        }

        Sort(samples.begin(), samples.end(), [](const TDeviceIoSample& a, const TDeviceIoSample& b) {
            return a.CompleteCycles < b.CompleteCycles;
        });

        for (const TDeviceIoSample& sample : samples) {
            if (sample.Size == 0) {
                // No zero-size (flush-like) samples are expected from uring
                // sources today; skip defensively if one shows up.
                continue;
            }

            // Mirrors TSharedCallback::Exec's isSeekExpected heuristic exactly,
            // including its mixing of cycle and nanosecond units -- kept as-is
            // for consistency with the legacy PDisk-only computation.
            bool isSeekExpected = (i64)(sample.SubmitCycles + seekCostNs / 25ull) >= (i64)PrevCompleteCycles;
            if (sample.Offset != EndOffset) {
                isSeekExpected = true;
            }
            EndOffset = sample.Offset + sample.Size;

            const i64 startCycle = Max<i64>(sample.SubmitCycles, PrevCompleteCycles);
            const i64 durationCycles = ((i64)sample.CompleteCycles > startCycle)
                ? (i64)sample.CompleteCycles - startCycle
                : 0;

            ui64 totalCostNs = sample.BaseCostNs;
            if (isSeekExpected) {
                totalCostNs += seekCostNs;
            }

            result.EstimatedNs += totalCostNs;
            result.ActualNs += HPNanoSeconds(durationCycles);
            ++result.SampleCount;

            PrevCompleteCycles = sample.CompleteCycles;
        }

        return result;
    }

private:
    const ui64 MaxBufferedSamples;

    mutable TMutex Mutex;
    TVector<TDeviceIoSample> Buffered;
    ui64 DroppedSamples = 0;

    // Continuity state carried across ComputeAndReset() calls, mirroring
    // TSharedCallback's PrevEventGotAtCycle/EndOffset member fields.
    ui64 EndOffset = 0;
    i64 PrevCompleteCycles = 0;
};

} // namespace NKikimr::NPDisk

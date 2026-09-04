#pragma once

#include <util/system/types.h>

namespace NKikimr::NPDisk {

// A raw, source-agnostic sample of a single completed device I/O operation.
//
// Produced by every path that talks directly to a physical device (PDisk's own
// block device thread, DDisk's io_uring I/O thread, PersistentBuffer's
// io_uring I/O thread). Carries just enough information for a downstream
// aggregator to recompute, for a globally completion-ordered merge of samples
// from multiple sources sharing one physical device:
//   - the actual duration of the operation (via a parallelism-1 model), and
//   - the estimated ("expected") cost of the operation, so the two can be
//     compared to derive a device overestimation ratio.
//
// SubmitCycles/CompleteCycles are NHPTimer cycle counts (see hp_timer_helpers.h),
// comparable only among samples produced on hosts with the same clock rate
// (i.e. within one process). They are not wall-clock timestamps.
struct TDeviceIoSample {
    ui64 SubmitCycles = 0;
    ui64 CompleteCycles = 0;

    // Absolute device (file) byte offset and size of the operation.
    ui64 Offset = 0;
    ui64 Size = 0;

    bool IsWrite = false;

    // Estimated ("expected") cost of the operation in nanoseconds, as modeled
    // by whoever produced the sample (excluding any additional seek cost that
    // the merge/aggregation step may apply based on cross-source ordering).
    ui64 BaseCostNs = 0;
};

} // namespace NKikimr::NPDisk

#pragma once

#include <util/system/types.h>

#if defined(__linux__)
#include <sys/uio.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#endif

namespace NActors {
    class TActorSystem;
} // namespace NActors

namespace NKikimr::NPDisk {

// Callers derive from this and add their own context fields.
// Should be allocated from a pool to avoid dynamic allocation in the hot path.
class TUringOperationBase {
    friend class TUringRouter;

public:
    // NHPTimer cycle count captured by TUringRouter right before the operation
    // is submitted to the kernel (SQE prepared). Used together with the
    // completion timestamp (captured by the I/O thread) to build a
    // TDeviceIoSample for device-overestimation tracking. 0 if unset.
    ui64 SubmitCycles = 0;

public:
    enum EOperationType {
        ENOT_SET = 0,
        EREAD,
        EWRITE,
    };

    virtual ~TUringOperationBase();

public:
    // Callbacks

    // Called from the dedicated I/O thread outside actor system,
    // thus MUST NOT use TActivationContext, instead should use actorSystem->Send().
    // After OnComplete() returns, TUringRouter will not access object anymore.
    virtual void OnComplete(NActors::TActorSystem* actorSystem) noexcept = 0;

    // Cleanup callback for abortive/failure teardown paths. Graceful
    // TUringRouter::Stop() drains every accepted operation through OnComplete.
    // Use this to release operation-owned memory/resources if an owner must
    // explicitly abandon an operation after a terminal router failure.
    // After OnDrop() returns, TUringRouter will not access object anymore.
    virtual void OnDrop() noexcept = 0;

public:
    // Prepare a single-buffer I/O.
    // buf must remain valid until OnComplete/OnDrop is called.
    void PrepareIov(void* buf, size_t size, ui64 offset);

#if defined(__linux__)
    // Begin a scatter-gather I/O: clears the iovec list, reserves room for
    // `count` segments and sets the disk offset.  Follow with `count` AddIov()
    // calls to append each segment.  count must be in (0, MAX_IOVS].
    void PrepareScatterGather(size_t count, ui64 offset);

    // Append one segment to the scatter-gather list started by
    // PrepareScatterGather.  buf must remain valid until OnComplete/OnDrop is
    // called.  Accumulates into TotalSize.
    void AddIov(void* buf, size_t size);
#endif

    void AdvanceIov(size_t bytesProcessed);

    void SetOperationType(EOperationType opType) { OperationType = opType; }
    EOperationType GetOperationType() const { return OperationType; }

    bool IsFixedBuffer() const { return FixedBuffer; }
    ui16 GetBufIndex() const { return BufIndex; }

    // Returns the number of bytes remaining in the current (possibly partially
    // advanced) iovec window — used by OnComplete to detect short I/O.
    // Invariant: GetOperationBytes() == TotalSize - BytesProcessed.
    size_t GetOperationBytes() const {
#if defined(__linux__)
        return TotalSize - BytesProcessed;
#else
        return GetTotalSize();
#endif
    }

    void SetResult(i32 result) { Result = result; }
    i32 GetResult() const { return Result; }

    ui64 GetTotalSize() const { return TotalSize; }

    ui64 GetDiskOffset() const { return DiskOffset; }

    const void* GetIovBase() const {
#if defined(__linux__)
        if (IovBegin < Iov.size()) {
            return Iov[IovBegin].iov_base;
        }
        return nullptr;
#else
        return nullptr;
#endif
    }

    // Reset all submission/completion state so the object can be reused from a pool.
    // Must be called before PrepareIov() when recycling an operation.
    void ResetSubmissionState() {
        SubmitCycles = 0;
        Result = 0;
        OperationType = ENOT_SET;
        TotalSize = 0;
        DiskOffset = 0;
        FixedBuffer = false;
        BufIndex = 0;
#if defined(__linux__)
        Iov.clear();
        IovBegin = 0;
        BytesProcessed = 0;
#endif
    }

#if defined(__linux__)
    // Number of iovecs kept inline (on-stack) without heap allocation.
    static constexpr size_t MAX_STACK_IOVS = 16;

    // Hard upper bound on scatter-gather segments per operation.  Beyond
    // MAX_STACK_IOVS the iovec vector spills to the heap, so this can exceed it.
    static constexpr size_t MAX_IOVS = 64;
#endif

private:
    // Set by TUringRouter::ReadFixed/WriteFixed before the operation is handed
    // to the I/O thread.
    void SetFixedBuffer(ui16 bufIndex) {
        FixedBuffer = true;
        BufIndex = bufIndex;
    }

    // Filled by TUringRouter on completion
    i32 Result = 0;  // io_uring cqe->res: bytes transferred on success, -errno on failure

    // Submission metadata for non-fixed Read/Write operations.

    EOperationType OperationType = ENOT_SET;

    // Originally requested byte count; set by PrepareIov/PrepareScatterGather/AddIov
    // and preserved across short-I/O retries so OnComplete knows the full request size.
    // Reset to 0 by ResetSubmissionState() when the op is recycled.
    ui64 TotalSize = 0;

    ui64 DiskOffset = 0;

    // Fixed-buffer operations must remember their registered-buffer index
    // until the I/O thread prepares the SQE.
    bool FixedBuffer = false;
    ui16 BufIndex = 0;

#if defined(__linux__)
    // Iovec array for readv/writev submissions.  Supports scatter-gather: holds one
    // entry for single-buffer I/O or N entries for multi-segment writes.
    // All iov_base pointers must remain valid until OnComplete/OnDrop is called.
    TStackVec<struct iovec, MAX_STACK_IOVS> Iov;

    // Index into Iov of the first not-yet-completed iovec.
    // Advanced by AdvanceIov() on short I/O retries.
    size_t IovBegin = 0;

    // Cumulative bytes consumed by AdvanceIov() across short-I/O retries.
    // GetOperationBytes() == TotalSize - BytesProcessed (remaining window).
    ui64 BytesProcessed = 0;
#endif
};

} // namespace NKikimr::NPDisk

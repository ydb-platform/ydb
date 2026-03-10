#pragma once

#include <util/system/types.h>

#if defined(__linux__)
#include <sys/uio.h>
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
    enum EOperationType {
        ENOT_SET = 0,
        EREAD,
        EWRITE,
    };

    virtual ~TUringOperationBase();

public:
    // Callbacks

    // Called from the dedicated completion polling thread outside actor system,
    // thus MUST NOT use TActivationContext, instead should use actorSystem->Send().
    // After OnComplete() returns, TUringRouter will not access object anymore.
    virtual void OnComplete(NActors::TActorSystem* actorSystem) noexcept = 0;

    // A cleanup callback called by TUringRouter::Stop() for CQEs drained
    // after shutdown without invoking OnComplete. Use this to release operation-
    // owned memory/resources for in-flight requests that are no longer delivered.
    // After OnDrop() returns, TUringRouter will not access object anymore.
    virtual void OnDrop() noexcept = 0;

public:
    // note, that buf should be valid until operation is finished:
    // normally only tests should use this "externally", while
    // derived classes keep the buf and use this to prepare I/O
    void PrepareIov(void* buf, size_t size, ui64 offset);

    void AdvanceIov(size_t bytesProcessed);

    void SetOperationType(EOperationType opType) { OperationType = opType; }
    EOperationType GetOperationType() const { return OperationType; }

    size_t GetOperationBytes() const {
#if defined(__linux__)
        return Iov.iov_len;
#else
        return GetTotalSize();
#endif
    }

    void SetResult(i32 result) { Result = result; }
    i32 GetResult() const { return Result; }

    ui64 GetTotalSize() const { return TotalSize; }

private:
    // Filled by TUringRouter on completion
    i32 Result = 0;  // io_uring cqe->res: bytes transferred on success, -errno on failure

    // Submission metadata for non-fixed Read/Write operations.

    EOperationType OperationType = ENOT_SET;

    // never changes after set: needed in case of short read/write to have
    // initially requested size
    ui64 TotalSize = 0;

    ui64 DiskOffset = 0;

#if defined(__linux__)
    // Scratch space for the iovec used by readv/writev submissions.
    // Populated by TUringRouter user and must remain valid until OnComplete is called.
    struct iovec Iov = {};
#endif
};

} // namespace NKikimr::NPDisk

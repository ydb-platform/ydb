#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_mask.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// An interface for interacting with DirtyMap. It is needed to register
// ready-to-process PBuffer records and update statistics about space usage in
// PBuffers.
struct IReadyQueue
{
    enum class EQueueType
    {
        Clone,
        Flush,
        Erase,
    };

    enum class EPBufferCounter
    {
        Total,
        Locked,
    };

    virtual ~IReadyQueue() = default;

    // Registers an Lsn ready for cloning, flushing, or erasing.
    // An Lsn can only be registered in one queue. The new registration deletes
    // the old one.
    virtual void Register(ui64 lsn, EQueueType queueType) = 0;

    // Removes all registrations from Lsn.
    virtual void UnRegister(ui64 lsn) = 0;

    // Notification about the change of byte counters in PBuffer
    virtual void DataToPBufferAdded(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) = 0;
    // Notification about the change of byte counters in PBuffer
    virtual void DataFromPBufferReleased(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadSource
{
    THostMask Mask;
    // true -> mask is DDisk hosts; false -> mask is PB hosts
    bool FromDDisk = false;
};

class TInflightInfo: public TDisableCopy
{
public:
    enum class EState
    {
        // During the recovery, a item without quorum was detected. It must be
        // copied to other PBuffers.
        // Reading will be possible only after receiving a quorum.
        PBufferIncompleteWrite,

        // Data written to PBuffers with quorum.
        // Read from any confirmed PBuffer.
        PBufferWritten,

        // Started flushing from PBuffers to DDisk.
        // Read from any confirmed PBuffer.
        PBufferFlushing,

        // Data flushed to DDisk.
        // Read from DDisk.
        PBufferFlushed,

        // The data is now being erasing from the PBuffers.
        // Read from DDisk.
        PBufferErasing,

        // The data is erased from the PBuffers.
        // Read from DDisk.
        PBufferErased,
    };

    TInflightInfo(
        IReadyQueue* readyQueues,
        ui64 lsn,
        size_t byteCount,
        THostIndex host);
    TInflightInfo(
        IReadyQueue* readyQueues,
        ui64 lsn,
        size_t byteCount,
        THostMask writeRequested,
        THostMask writeConfirmed);

    TInflightInfo(TInflightInfo&& other) noexcept;

    ~TInflightInfo();

    // Detach from ReadyQueue.
    void Detach();

    void RestorePBuffer(THostIndex host);

    [[nodiscard]] EState GetState() const;

    // The subscription is triggered when the quorum is reached.
    [[nodiscard]] NThreading::TFuture<void> GetQuorumReadyFuture();

    // The mask from which data sources can be read. The allDDisks parameter
    // is used as the read mask once the data has been transferred to DDisks.
    [[nodiscard]] TReadSource ReadMask(THostMask allDDisks) const;

    // Returns the PBuffer source from where the data will be transferred to
    // DDisk, specified in the parameter destination. If InvalidHostIndex is
    // returned, it means that the transfer of data to destination has already
    // been requested earlier.
    [[nodiscard]] THostIndex RequestFlush(THostIndex destination);
    void ConfirmFlush(THostRoute route);
    void FlushFailed(THostRoute route);
    [[nodiscard]] THostMask GetRequestedFlushes() const;

    // Returns true when erase request needed.
    [[nodiscard]] bool RequestErase(THostIndex host);
    // Returns true when all erases confirmed.
    [[nodiscard]] bool ConfirmErase(THostIndex host);
    void EraseFailed(THostIndex host);

    // Sets a lock that prohibits erasing the PBuffer.
    void LockPBuffer();
    // Removes the lock that prohibits erasing the PBuffer.
    void UnlockPBuffer();

private:
    void ApplyBytes(
        THostIndex host,
        IReadyQueue::EPBufferCounter counter,
        bool add) const;
    void ApplyBytes(
        THostMask mask,
        IReadyQueue::EPBufferCounter counter,
        bool add) const;

    EState State;

    IReadyQueue* ReadyQueue = nullptr;
    ui64 Lsn = 0;
    size_t ByteCount = 0;
    TInstant StartAt;
    size_t PBuffersLockCount = 0;
    NThreading::TPromise<void> QuorumReadyPromise;

    THostMask WriteRequested;
    THostMask WriteConfirmed;
    THostMask FlushDesired;
    THostMask FlushRequested;
    THostMask FlushConfirmed;
    THostMask EraseRequested;
    THostMask EraseConfirmed;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

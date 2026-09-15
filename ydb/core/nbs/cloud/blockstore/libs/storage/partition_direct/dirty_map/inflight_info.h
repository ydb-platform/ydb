#pragma once

<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
#include "public.h"

>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/pbuffer_key.h>
>>>>>>> 78a20188184 (Field map set optimization (#52565))
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

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

<<<<<<< HEAD
    // Registers an Lsn ready for cloning, flushing, or erasing.
    // An Lsn can only be registered in one queue. The new registration deletes
    // the old one.
    virtual void Register(ui64 lsn, EQueueType queueType) = 0;
=======
    [[nodiscard]] virtual TPBufferKey GetPBufferKey(
        const TInflightInfo& inflight) const = 0;

    // Registers a record ready for cloning, flushing, or erasing.
    // A record can only be registered in one queue. The new registration
    // deletes the old one.
    virtual void Register(
        const TInflightInfo& inflight,
        EQueueType queueType) = 0;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

<<<<<<< HEAD
    // Removes all registrations from Lsn.
    virtual void UnRegister(ui64 lsn) = 0;
=======
    // Removes the record's registration from the given queue.
    virtual void UnRegister(
        const TInflightInfo& inflight,
        EQueueType queueType) = 0;

    // Notifies that a flush request to the specified host stopped being
    // in-flight. The request may have completed successfully, failed, or been
    // dropped because the host was disabled.
    virtual void InflightFlushFinished(
        const TInflightInfo& inflight,
        THostIndex host) = 0;

    // Notifies of flushes completion to DDisks.
<<<<<<< HEAD
    virtual void FlushCompleted(TPBufferKey pBufferKey, THostMask ddisks) = 0;
>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
=======
    virtual void FlushCompleted(
        const TInflightInfo& inflight,
        THostMask ddisks) = 0;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    // Notification about the change of byte counters in PBuffer
    virtual void DataToPBufferAdded(
        const TInflightInfo& inflight,
        THostIndex host,
        EPBufferCounter counter) = 0;
    // Notification about the change of byte counters in PBuffer
    virtual void DataFromPBufferReleased(
        const TInflightInfo& inflight,
        THostIndex host,
        EPBufferCounter counter) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadSource
{
    THostMask Mask;
    // 0 -> read from DDisk (Mask is the set of DDisk hosts to read from).
    // >0 -> read from a PBuffer that holds the inflight write at this lsn
    // (Mask is the set of PBuffer hosts that confirmed the write).
    ui64 Lsn = 0;

    [[nodiscard]] bool Empty() const
    {
        return Mask.Empty();
    }

    [[nodiscard]] bool OnlyDDisk() const
    {
        return Lsn == 0;
    }
};

class TInflightInfo: public TDisableCopy
{
public:
    enum class EState: ui8
    {
        // The lsn is generated but the write has not been acknowledged yet.
        // Tracked only to hold the cleanup watermark; invisible to reads (a
        // concurrent read sees the pre-write data on DDisk, as before).
        PBufferPendingWrite,

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
<<<<<<< HEAD
        IReadyQueue* readyQueues,
        ui64 lsn,
        size_t byteCount,
        THostIndex host);

    // Pending write: lsn is generated but data is not in any PBuffer yet.
    // ReadMask is empty (reads wait on the quorum future) and the write is not
    // flushable. Call OnWritten once a quorum of PBuffers confirms the write.
    TInflightInfo(IReadyQueue* readyQueue, ui64 lsn, size_t byteCount);
=======
        IReadyQueue* readyQueue,
        THostMask desiredDDisks,
<<<<<<< HEAD
        THostMask disabled,
        TPBufferKey pBufferKey,
        size_t byteCount);
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
        THostMask disabled);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    TInflightInfo(TInflightInfo&& other) noexcept;

    ~TInflightInfo();

    // Detach from ReadyQueue.
    void Detach();

    void RestorePBuffer(THostIndex host);

    // Transitions a pending write (see the byteCount-only constructor) to the
    // written state once a quorum of PBuffers confirmed it.
    void OnWritten(THostMask writeRequested, THostMask writeConfirmed);

    [[nodiscard]] EState GetState() const;

    // The subscription is triggered when the quorum is reached.
    [[nodiscard]] NThreading::TFuture<void> GetQuorumReadyFuture();

    // The mask from which data sources can be read.
    [[nodiscard]] TReadSource ReadMask() const;

    // Returns the PBuffer source from where the data will be transferred to
    // DDisk, specified in the parameter destination. If InvalidHostIndex is
    // returned, it means that the transfer of data to destination has already
    // been requested earlier.
    [[nodiscard]] THostIndex RequestFlush(
        THostIndex destination,
        THostMask disabledHosts);
    void ConfirmFlush(THostIndex host);
    void FlushFailed(THostIndex host);
    [[nodiscard]] THostMask GetRequestedFlushes() const;

    void RequestErase(THostIndex host);
    void ConfirmErase(THostIndex host);
    void EraseFailed(THostIndex host);
    // Hosts where a write was requested but erase is not yet
    // requested/confirmed.
    [[nodiscard]] THostMask GetEraseNeeded() const;

    // Skip removed hosts flushing and erase. Update state.
    void RemoveHosts(THostMask removed);

    // Sets a lock that prohibits erasing the PBuffer.
    void LockPBuffer();
    // Removes the lock that prohibits erasing the PBuffer.
    void UnlockPBuffer();

    TString DebugPrint(TInstant now) const;

private:
    void ApplyBytes(
        THostIndex host,
        IReadyQueue::EPBufferCounter counter,
        bool add) const;
    void ApplyBytes(
        THostMask mask,
        IReadyQueue::EPBufferCounter counter,
        bool add) const;

    void SetState(EState newState);
    void CheckInvariants() const;

<<<<<<< HEAD
    EState State;

    IReadyQueue* ReadyQueue = nullptr;
    ui64 Lsn = 0;
    size_t ByteCount = 0;
=======
    void MaybeAdvanceToFlushed();
    void MaybeAdvanceToErased();
    void MaybeQueryErase();

    [[nodiscard]] TPBufferKey GetPBufferKey() const;

    // EState has 7 values, so 3 bits are enough to store it. The rest of the
    // ui32 word is given to PBuffersLockCount to maximize its capacity.
    static constexpr ui32 StateBits = 3;
    static_assert(
        static_cast<ui32>(EState::PBufferErased) < (1U << StateBits),
        "EState values do not fit into the State bit width");

    IReadyQueue* ReadyQueue = nullptr;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    TInstant StartAt;
    NThreading::TPromise<void> QuorumReadyPromise;
<<<<<<< HEAD
=======
    ui32 PersistGeneration = 0;
    ui32 PBuffersLockCount : 32 - StateBits = 0;
    EState State: StateBits = EState::PBufferPendingWrite;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

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

#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/pbuffer_key.h>
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

    [[nodiscard]] virtual TPBufferKey GetPBufferKey(
        const TInflightInfo& inflight) const = 0;

    // Registers a record ready for cloning, flushing, or erasing.
    // A record can only be registered in one queue. The new registration
    // deletes the old one.
    virtual void Register(
        const TInflightInfo& inflight,
        EQueueType queueType) = 0;

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
    virtual void FlushCompleted(
        const TInflightInfo& inflight,
        THostMask ddisks) = 0;

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
    // PBufferKey.Lsn == 0 -> read from DDisk (Mask is the set of DDisk hosts to
    // read from).
    // PBufferKey.Lsn > 0 -> read from a PBuffer that holds this inflight record
    // (Mask is the set of PBuffer hosts that confirmed the write).
    TPBufferKey PBufferKey;

    [[nodiscard]] bool Empty() const
    {
        return Mask.Empty();
    }

    [[nodiscard]] bool OnlyDDisk() const
    {
        return PBufferKey.Lsn == 0;
    }
};

class TInflightInfo: public TDisableCopy
{
public:
    /*
     * PBufferPendingWrite -- OnWriteWithoutQuorum --> PBufferDiscarded -----+
     *                                                      erase or barrier |
     * PBufferPendingWrite -- RestorePBuffer --> PBufferIncompleteWrite      |
     *                        below quorum         |                         |
     *                                             | RestorePBuffer          |
     *                                             | confirms the third copy |
     *                                             v                         |
     * PBufferPendingWrite -- OnWritten --> PBufferWritten                   |
     *                                             |                         |
     *                                             | RequestFlush            |
     *                                             v                         |
     *                                      PBufferFlushing                  |
     *                                             |                         |
     *                                             | MaybeAdvanceToFlushed   |
     *                                             v                         |
     *                                      PBufferFlushed                   |
     *                                             |                         |
     *                                             | erase or barrier        |
     *                                             v                         |
     *                                       PBufferErased <-----------------+
     */
    enum class EState: ui8
    {
        // The lsn is generated but the write has not been acknowledged yet.
        // Tracked only to hold the cleanup watermark; invisible to reads (a
        // concurrent read sees the pre-write data on DDisk, as before).
        PBufferPendingWrite,

        // Below quorum while the restore list is still being applied. A read
        // waits. FinishPBufferRestore discards whatever is still below quorum.
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

        // The write got no quorum: the client got an error and this record
        // will never be flushed to DDisk. The copies that did land still
        // have to be erased, the same way as after a flush.
        // Read from DDisk.
        PBufferDiscarded,

        // Erased from the PBuffers or covered by the restore barrier.
        // Reached from PBufferFlushed and from PBufferDiscarded.
        // Read from DDisk.
        PBufferErased,
    };

    TInflightInfo(
        IReadyQueue* readyQueue,
        THostMask desiredDDisks,
        THostMask disabled);

    TInflightInfo(TInflightInfo&& other) noexcept;

    ~TInflightInfo();

    // Detach from ReadyQueue. Called before parent DirtyMap destroyed.
    void Detach();

    // Instance of PBuffer record found on host during recovery.
    void RestorePBuffer(THostIndex host);

    // The restore list is finished and this record is still below quorum.
    // Copies that were found are erased by address. Every other host stays
    // unconfirmed, so the restore barrier finishes the record.
    void DiscardBelowQuorum(THostMask allHosts);

    // Transitions a pending write to the written state once a quorum of
    // PBuffers confirmed it. `writeAnswered` are the hosts with no write
    // request left in flight.
    void OnWritten(
        THostMask writeRequested,
        THostMask writeConfirmed,
        THostMask writeAnswered);

    // The quorum was not reached and the client got an error. Confirmed
    // copies still have to be erased. A host that did not confirm is
    // finished by the restore barrier.
    void OnWriteWithoutQuorum(
        THostMask writeRequested,
        THostMask writeConfirmed,
        THostMask writeAnswered);

    // Answers that came after the client had been replied to. A belated
    // success confirms the host, and only then is it erased.
    void OnBelatedWrite(THostMask completed, THostMask failed);

    [[nodiscard]] EState GetState() const;

    // The subscription is triggered when the quorum is reached.
    [[nodiscard]] NThreading::TFuture<void> GetQuorumReadyFuture();

    // The mask from which data sources can be read.
    [[nodiscard]] TReadSource ReadMask() const;

    // Returns the PBuffer source from where the data will be transferred to
    // DDisk, specified in the parameter destination. If InvalidHostIndex is
    // returned, it means that the transfer of data to destination has already
    // been requested earlier.
    [[nodiscard]] THostIndex RequestFlush(THostIndex destination);
    void ConfirmFlush(THostIndex host);
    void FlushFailed(THostIndex host);
    [[nodiscard]] THostMask GetInflightFlushes() const;

    void RequestErase(THostIndex host);
    void ConfirmErase(THostIndex host);
    void EraseFailed(THostIndex host);
    // Enabled hosts that confirmed the write and are not yet erased.
    [[nodiscard]] THostMask GetEraseNeeded() const;
    [[nodiscard]] bool IsDataOnlyInPBuffers() const;
    [[nodiscard]] bool CanBeCoveredByRestoreBarrier() const;
    // Marks the record erased once the persisted restore barrier covers it.
    void MarkCoveredByRestoreBarrier();

    // Update state according to the changed configuration.
    void UpdateHosts(THostMask added, THostMask removed, THostMask disabled);

    // Sets a lock that prohibits erasing the PBuffer.
    void LockPBuffer();
    // Removes the lock that prohibits erasing the PBuffer.
    void UnlockPBuffer();

    // The generation of the DirtyMap persisted state. If a generation has been
    // assigned, it means that erasing can only be started after saving of data
    // from that or higher generation in the partition local database.
    void SetPersistGeneration(ui32 persistGeneration);
    [[nodiscard]] ui32 GetPersistGeneration() const;

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

    void MaybeAdvanceToFlushed();
    void MaybeAdvanceToErased();
    void MaybeQueryErase();
    [[nodiscard]] bool CanErase() const;
    // Every requested host confirmed the erase. An unconfirmed host never
    // does, so the restore barrier is what finishes that record.
    [[nodiscard]] bool AllPBuffersErased() const;
    // Hosts that were asked to write and have not confirmed it. An error or a
    // missing answer does not prove that the copy will not land.
    [[nodiscard]] THostMask HostsWithUnconfirmedWrite() const;

    [[nodiscard]] TPBufferKey GetPBufferKey() const;

    // EState has 7 values, so 3 bits are enough to store it. The rest of the
    // ui32 word is given to PBuffersLockCount to maximize its capacity.
    static constexpr ui32 StateBits = 3;
    static_assert(
        static_cast<ui32>(EState::PBufferErased) < (1U << StateBits),
        "EState values do not fit into the State bit width");

    IReadyQueue* ReadyQueue = nullptr;
    TInstant StartAt;
    NThreading::TPromise<void> QuorumReadyPromise;
    ui32 PersistGeneration = 0;
    ui32 PBuffersLockCount : 32 - StateBits = 0;
    EState State: StateBits = EState::PBufferPendingWrite;

    THostMask DesiredDDisks;
    THostMask Disabled;
    THostMask WriteRequested;
    THostMask WriteConfirmed;
    THostMask WriteAnswered;
    THostMask FlushRequested;
    THostMask FlushConfirmed;
    THostMask EraseRequested;
    THostMask EraseConfirmed;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

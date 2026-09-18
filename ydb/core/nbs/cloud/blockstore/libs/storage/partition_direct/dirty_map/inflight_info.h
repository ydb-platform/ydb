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
    enum class EState: ui8
    {
        // The lsn is generated but the write has not been acknowledged yet.
        // Tracked only to hold the cleanup watermark; invisible to reads (a
        // concurrent read sees the pre-write data on DDisk, as before).
        PBufferPendingWrite,

        // During the recovery, an item without quorum was detected. It must be
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

        // The write got no quorum: the client got an error and this record
        // will never be flushed to DDisk. The copies that did land still
        // have to be erased, the same way as after a flush.
        // Read from DDisk.
        PBufferDiscarded,

        // No copy is left on any PBuffer and every host has answered, so the
        // record can leave the map. Reached from PBufferFlushed and from
        // PBufferDiscarded.
        // Read from DDisk.
        PBufferErased,
    };

    // The state says what happened to the data. What happened to the copies
    // is kept in the masks: EraseRequested, EraseConfirmed,
    // EraseSentBeforeAnswer. Erasing is allowed in PBufferFlushed (the data is
    // on DDisk already) and in PBufferDiscarded (the write was answered with an
    // error, so erasing a copy cannot lose anything), and nowhere else.

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

    // Transitions a pending write to the written state once a quorum of
    // PBuffers confirmed it. Answered are the hosts with no write request
    // in flight: a confirmed host is not answered while another write to it
    // can still land.
    void OnWritten(
        THostMask writeRequested,
        THostMask writeConfirmed,
        THostMask writeAnswered);

    // Transitions a pending write to PBufferDiscarded: the quorum was not
    // reached and the client got an error, but the copies that did land
    // still have to be erased.
    void OnWriteWithoutQuorum(
        THostMask writeRequested,
        THostMask writeConfirmed,
        THostMask writeAnswered);

    // Answers that came after the client had been replied to, successful and
    // failed. Only an erase sent after such an answer proves that the host
    // holds no copy: a confirmed erase is repeated, an erase in flight is
    // marked and repeated once it answers.
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
    // Hosts where a write was requested but erase is not yet
    // requested/confirmed.
    [[nodiscard]] THostMask GetEraseNeeded() const;

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
    // Moves the record to PBufferErased when nothing is left to erase. Works
    // both for a record that was flushed and for one kept only for its copies.
    void MaybeAdvanceToErased();
    void MaybeQueryErase();

    // True while erasing copies is allowed: the data is on DDisk or the
    // write was answered with an error.
    [[nodiscard]] bool CanErase() const;
    // Drops an erase attempt that proved nothing and asks for a new one.
    void RetryErase(THostIndex host);
    // True when every host that was asked to write has answered and every
    // copy has been erased, so the record can leave the map.
    [[nodiscard]] bool CanForget() const;

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
    // Hosts with no write request in flight, whatever they answered. Only
    // such a host counts as answered for CanForget.
    THostMask WriteAnswered;
    THostMask FlushRequested;
    THostMask FlushConfirmed;
    THostMask EraseRequested;
    THostMask EraseConfirmed;
    // Hosts whose erase in flight was sent before the host answered the
    // write. Such an erase may have run before the copy landed, so its
    // answer is not counted and the erase is repeated.
    THostMask EraseSentBeforeAnswer;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

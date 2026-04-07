#pragma once

#include "datashard.h"
#include <ydb/core/tx/locks/locks.h>

#include <ydb/core/base/row_version.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

#include <util/digest/multi.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace NKikimr::NDataShard {

struct TReadIteratorId {
    TActorId Sender;
    ui64 ReadId;

    TReadIteratorId(const TActorId& sender, ui64 readId)
        : Sender(sender)
        , ReadId(readId)
    {}

    bool operator ==(const TReadIteratorId& rhs) const = default;

    struct THash {
        size_t operator ()(const TReadIteratorId& id) const {
            return MultiHash(id.Sender.Hash(), id.ReadId);
        }
    };

    TString ToString() const {
        TStringStream ss;
        ss << "{" << Sender << ", " << ReadId << "}";
        return ss.Str();
    }
};

struct TReadIteratorSession {
    TReadIteratorSession() = default;
    THashSet<TReadIteratorId, TReadIteratorId::THash> Iterators;
};

struct TReadIteratorVectorTop;

struct TReadIteratorState {
    enum class EState {
        Init,
        Executing,
        Exhausted,
        Scan,
    };

    struct TQuotaValues {
        TQuotaValues(ui64 rows = Max<ui64>(), ui64 bytes = Max<ui64>())
            : Rows(rows)
            , Bytes(bytes)
        {}

        ui64 Rows;
        ui64 Bytes;
    };

    struct TQuota : public TQuotaValues {
        ui64 SeqNo = 0;
        ui64 LastAckSeqNo = 0;

        // Unacknowledged quota values (rolling sum)
        // first item corresponds to SeqNo = LastAckSeqNo + 1,
        // the full deque corresponds to range [LastAckSeqNo + 1; SeqNo]
        std::deque<TQuotaValues> Queue;

        /**
         * Consume the specified number of rows and bytes, allocating a new SeqNo.
         *
         * Returns true when there is any quota left.
         */
        bool Consume(ui64 rows, ui64 bytes) {
            ++SeqNo;

            ui64 lastRowsTotal = !Queue.empty() ? Queue.back().Rows : 0;
            ui64 lastBytesTotal = !Queue.empty() ? Queue.back().Bytes : 0;
            Queue.emplace_back(lastRowsTotal + rows, lastBytesTotal + bytes);

            Rows = Rows > rows ? Rows - rows : 0;
            Bytes = Bytes > bytes ? Bytes - bytes : 0;

            return Rows > 0 && Bytes > 0;
        }

        /**
         * Acknowledges a previously consumed SeqNo and specifies a new total
         * quota immediately after that SeqNo. This is usually the remaining
         * buffer space in the reader.
         *
         * Returns true when there is any quota left.
         */
        bool Ack(ui64 seqNo, ui64 rows, ui64 bytes) {
            if (LastAckSeqNo < seqNo && seqNo <= SeqNo) {
                size_t ackedIndex = seqNo - LastAckSeqNo - 1;
                Y_ENSURE(ackedIndex < Queue.size());

                auto it = Queue.begin() + ackedIndex;

                // How many rows and bytes are still consumed (unacknowledged)
                ui64 consumedRows = Queue.back().Rows - it->Rows;
                ui64 consumedBytes = Queue.back().Bytes - it->Bytes;
                Queue.erase(Queue.begin(), ++it);

                Rows = rows > consumedRows ? rows - consumedRows : 0;
                Bytes = bytes > consumedBytes ? bytes - consumedBytes : 0;

                LastAckSeqNo = seqNo;
            } else if (seqNo == SeqNo) {
                Rows = rows;
                Bytes = bytes;
            }

            return Rows > 0 && Bytes > 0;
        }
    };

public:
    TReadIteratorState(
            const TReadIteratorId& readId, ui64 localReadId, const TPathId& pathId,
            const TActorId& sessionId, const TRowVersion& readVersion, bool isHeadRead,
            TMonotonic ts)
        : ReadId(readId)
        , LocalReadId(localReadId)
        , PathId(pathId)
        , ReadVersion(readVersion)
        , IsHeadRead(isHeadRead)
        , SessionId(sessionId)
        , StartTs(ts)
    {}

    TReadIteratorState(const TReadIteratorState&) = delete;
    TReadIteratorState& operator=(const TReadIteratorState&) = delete;

    bool IsExhausted() const { return State == EState::Exhausted; }

    // must be called only once per SeqNo
    void ConsumeSeqNo(ui64 rows, ui64 bytes) {
        if (!Quota.Consume(rows, bytes)) {
            State = EState::Exhausted;
        }
        SeqNo = Quota.SeqNo;
    }

    void UpQuota(ui64 ackSeqNo, ui64 rows, ui64 bytes) {
        if (Quota.Ack(ackSeqNo, rows, bytes)) {
            State = EState::Executing;
        } else {
            State = EState::Exhausted;
        }
        LastAckSeqNo = Quota.LastAckSeqNo;
    }

    void ForwardScanEvent(std::unique_ptr<IEventHandle>&& ev, ui64 tabletId);

public:
    EState State = EState::Init;

    // Data from original request //

    const TReadIteratorId ReadId;
    const ui64 LocalReadId;
    const TPathId PathId;
    std::vector<NTable::TTag> Columns;
    TRowVersion ReadVersion;
    bool IsHeadRead;
    ui64 LockId = 0;
    ui32 LockNodeId = 0;
    ui64 QuerySpanId = 0;
    NKikimrDataEvents::ELockMode LockMode = NKikimrDataEvents::OPTIMISTIC;
    TLockInfo::TPtr Lock;
    bool LockInconsistent = false;

    // note that will be always overwritten by values from request
    NKikimrDataEvents::EDataFormat Format = NKikimrDataEvents::FORMAT_CELLVEC;

    // mainly for tests
    ui64 MaxRowsInResult = Max<ui64>();

    ui64 SchemaVersion = 0;

    bool Reverse = false;

    // The original event handle
    TEvDataShard::TEvRead::TPtr Ev;
    TEvDataShard::TEvRead* Request = nullptr;

    // parallel to Request->Keys, but real data only in indices,
    // where in Request->Keys we have key prefix (here we have properly extended one).
    TVector<TSerializedCellVec> Keys;

    // State itself //

    TQuota Quota;

    // Number of rows processed so far
    ui64 TotalRows = 0;
    ui64 TotalRowsLimit = Max<ui64>();

    TActorId SessionId;
    TMonotonic StartTs;
    bool IsFinished = false;
    bool ReadContinuePending = false;

    // note that we send SeqNo's starting from 1
    ui64 SeqNo = 0;
    ui64 LastAckSeqNo = 0;
    ui64 FirstUnprocessedQuery = 0; // must be unsigned
    TString LastProcessedKey;
    bool LastProcessedKeyErased = false;

    // used when read is implemented with a scan
    ui64 ScanId = 0;
    ui64 ScanLocalTid = 0;
    TActorId ScanActorId;
    // temporary storage for forwarded events until scan has started
    std::vector<std::unique_ptr<IEventHandle>> ScanPendingEvents;

    // May be used to cancel enqueued transactions
    ui64 EnqueuedLocalTxId = 0;

    // Vector search pushdown
    std::shared_ptr<TReadIteratorVectorTop> VectorTopK;
};

using TReadIteratorsMap = THashMap<TReadIteratorId, TReadIteratorState, TReadIteratorId::THash>;
using TReadIteratorsLocalMap = THashMap<ui64, TReadIteratorState*>;

} // NKikimr::NDataShard

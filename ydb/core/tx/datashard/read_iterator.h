#pragma once

#include "datashard.h"

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
    ui64 ReadId = 0;

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

struct TReadIteratorState {
    enum class EState {
        Init,
        Executing,
        Exhausted,
    };

    struct TQuota {
        TQuota() = default;

        TQuota(ui64 rows, ui64 bytes)
            : Rows(rows)
            , Bytes(bytes)
        {}

        ui64 Rows = Max<ui64>();
        ui64 Bytes = Max<ui64>();
    };

public:
    explicit TReadIteratorState(const TActorId& sessionId)
        : SessionId(sessionId)
    {}

    bool IsExhausted() const { return State == EState::Exhausted; }

    // must be called only once per SeqNo
    void ConsumeSeqNo(ui64 rows, ui64 bytes) {
        ++SeqNo;
        ReadStats.emplace_back(rows, bytes);

        if (Quota.Rows <= rows) {
            Quota.Rows = 0;
        } else {
            Quota.Rows -= rows;
        }

        if (Quota.Bytes <= bytes) {
            Quota.Bytes = 0;
        } else {
            Quota.Bytes -= bytes;
        }

        if (Quota.Rows == 0 || Quota.Bytes == 0) {
            State = EState::Exhausted;
        }
    }

    void UpQuota(ui64 seqNo, ui64 rows, ui64 bytes) {
        Y_ASSERT(seqNo <= ReadStats.size());
        // user provided quota for seqNo, if we have sent messages
        // with higher seqNo then we should account their bytes to
        // this quota
        ui64 consumedBytes = 0, consumedRows = 0;
        for (ui64 i = seqNo; i < ReadStats.size(); ++i) {
            consumedRows += ReadStats[i].Rows;
            consumedBytes += ReadStats[i].Bytes;
        }

        if (consumedRows >= rows) {
            Quota.Rows = 0;
        } else {
            Quota.Rows = rows - consumedRows;
        }

        if (consumedBytes >= bytes) {
            Quota.Bytes = 0;
        } else {
            Quota.Bytes = bytes - consumedBytes;
        }

        if (Quota.Rows == 0 || Quota.Bytes == 0) {
            State = EState::Exhausted;
        } else {
            State = EState::Executing;
        }
    }

public:
    EState State = EState::Init;

    // Data from original request //

    ui64 ReadId = 0;
    TPathId PathId;
    std::vector<NTable::TTag> Columns;
    TRowVersion ReadVersion = TRowVersion::Max();
    ui64 LockTxId = 0;

    // note that will be always overwritten by values from request
    NKikimrTxDataShard::EScanDataFormat Format = NKikimrTxDataShard::EScanDataFormat::CELLVEC;

    // mainly for tests
    ui64 MaxRowsInResult = Max<ui64>();

    ui64 SchemaVersion = 0;

    bool Reverse = false;

    std::shared_ptr<TEvDataShard::TEvRead> Request;

    // State itself //

    TQuota Quota;
    TVector<TQuota> ReadStats; // each index corresponds to SeqNo-1

    TActorId SessionId;
    ui64 SeqNo = 0;
    ui64 LastAckSeqNo = 0;
    ui32 FirstUnprocessedQuery = 0;
    TString LastProcessedKey = 0;
};

using TReadIteratorStatePtr = std::unique_ptr<TReadIteratorState>;
using TReadIteratorsMap = std::unordered_map<TReadIteratorId, TReadIteratorStatePtr, TReadIteratorId::THash>;

} // NKikimr::NDataShard

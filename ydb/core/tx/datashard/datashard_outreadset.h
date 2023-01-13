#pragma once

#include <ydb/core/tx/tx_processing.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;

///
struct TReadSetKey {
    ui64 TxId;
    ui64 Origin;
    ui64 From;
    ui64 To;

    explicit TReadSetKey(ui64 txId = 0, ui64 origin = 0, ui64 from = 0, ui64 to = 0)
        : TxId(txId)
        , Origin(origin)
        , From(from)
        , To(to)
    {}

    TReadSetKey(const NKikimrTx::TEvReadSet& rs)
        : TxId(rs.GetTxId())
        , Origin(rs.GetTabletProducer())
        , From(rs.GetTabletSource())
        , To(rs.GetTabletDest())
    {}

    size_t Hash() const {
        return TxId + (Origin << 16) + (From << 8) + To;
    }

    explicit operator size_t () const {
        return Hash();
    }

    bool operator == (const TReadSetKey& other) const {
        return TxId == other.TxId && Origin == other.Origin && From == other.From && To == other.To;
    }
};

///
class TOutReadSets {
public:
    friend class TDataShard;

    TOutReadSets(TDataShard * self)
        : Self(self)
    {}

    bool LoadReadSets(NIceDb::TNiceDb& db);
    void SaveReadSet(NIceDb::TNiceDb& db, ui64 seqNo, ui64 step, const TReadSetKey& rsKey, TString body);
    void SaveAck(const TActorContext& ctx, TAutoPtr<TEvTxProcessing::TEvReadSetAck> ev);
    void AckForDeletedDestination(ui64 tabletId, ui64 seqNo, const TActorContext &ctx);
    bool ResendRS(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx, ui64 seqNo);
    void ResendAll(const TActorContext& ctx);
    void Cleanup(NIceDb::TNiceDb& db, const TActorContext& ctx);

    bool Empty() const { return CurrentReadSets.empty() && Expectations.empty(); }
    bool HasAcks() const { return ! ReadSetAcks.empty(); }
    bool Has(const TReadSetKey& rsKey) const { return CurrentReadSetInfos.contains(rsKey); }

    ui64 CountReadSets() const { return CurrentReadSets.size(); }
    ui64 CountAcks() const { return ReadSetAcks.size(); }

    bool AddExpectation(ui64 target, ui64 step, ui64 txId);
    bool RemoveExpectation(ui64 target, ui64 txId);
    bool HasExpectations(ui64 target);
    void ResendExpectations(ui64 target, const TActorContext& ctx);
    THashMap<ui64, ui64> RemoveExpectations(ui64 target);

private:
    void UpdateMonCounter() const;

private:
    TDataShard * Self;
    THashMap<ui64, TReadSetKey> CurrentReadSets;      // SeqNo -> Info
    THashMap<TReadSetKey, ui64> CurrentReadSetInfos;  // Info -> SeqNo
    THashSet<ui64> AckedSeqno;
    TVector<TIntrusivePtr<TEvTxProcessing::TEvReadSetAck>> ReadSetAcks;
    // Target -> TxId -> Step
    THashMap<ui64, THashMap<ui64, ui64>> Expectations;
};

}}

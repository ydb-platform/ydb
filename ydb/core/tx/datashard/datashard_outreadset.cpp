#include "defs.h"

#include "datashard_outreadset.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

void TOutReadSets::UpdateMonCounter() const {
    Self->SetCounter(COUNTER_OUT_READSETS_IN_FLIGHT, CurrentReadSets.size());
}

bool TOutReadSets::LoadReadSets(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    CurrentReadSets.clear(); // For idempotency
    CurrentReadSetInfos.clear();

    // TODO[serxa]: this should be Range but it is not working right now
    auto rowset = db.Table<Schema::OutReadSets>().GreaterOrEqual(0).Select<
                                    Schema::OutReadSets::Seqno,
                                    Schema::OutReadSets::TxId,
                                    Schema::OutReadSets::Origin,
                                    Schema::OutReadSets::From,
                                    Schema::OutReadSets::To>();
    if (!rowset.IsReady())
        return false;
    while (!rowset.EndOfSet()) {
        ui64 seqNo = rowset.GetValue<Schema::OutReadSets::Seqno>();
        ui64 txId = rowset.GetValue<Schema::OutReadSets::TxId>();
        ui64 origin = rowset.GetValue<Schema::OutReadSets::Origin>();
        ui64 source = rowset.GetValue<Schema::OutReadSets::From>();
        ui64 target = rowset.GetValue<Schema::OutReadSets::To>();

        TReadSetKey rsInfo(txId, origin, source, target);

        Y_VERIFY(!CurrentReadSets.contains(seqNo));
        Y_VERIFY(!CurrentReadSetInfos.contains(rsInfo));

        CurrentReadSets[seqNo] = rsInfo;
        CurrentReadSetInfos[rsInfo] = seqNo;

        if (!rowset.Next())
            return false;
    }

    UpdateMonCounter();
    return true;
}

void TOutReadSets::SaveReadSet(NIceDb::TNiceDb& db, ui64 seqNo, ui64 step, const TReadSetKey& rsInfo, TString body) {
    using Schema = TDataShard::Schema;

    Y_VERIFY(!CurrentReadSets.contains(seqNo));
    Y_VERIFY(!CurrentReadSetInfos.contains(rsInfo));

    CurrentReadSetInfos[rsInfo] = seqNo;
    CurrentReadSets[seqNo] = rsInfo;

    UpdateMonCounter();

    db.Table<Schema::OutReadSets>().Key(seqNo).Update(
        NIceDb::TUpdate<Schema::OutReadSets::Step>(step),
        NIceDb::TUpdate<Schema::OutReadSets::TxId>(rsInfo.TxId),
        NIceDb::TUpdate<Schema::OutReadSets::Origin>(rsInfo.Origin),
        NIceDb::TUpdate<Schema::OutReadSets::From>(rsInfo.From),
        NIceDb::TUpdate<Schema::OutReadSets::To>(rsInfo.To),
        NIceDb::TUpdate<Schema::OutReadSets::Body>(body));
}

void TOutReadSets::AckForDeletedDestination(ui64 tabletId, ui64 seqNo, const TActorContext &ctx) {
    const TReadSetKey* rsInfo  = CurrentReadSets.FindPtr(seqNo);

    if (!rsInfo) {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
            "Unknown seqNo %" PRIu64 " for readset to tablet %" PRIu64 " at tablet %" PRIu64,
            seqNo, tabletId, Self->TabletID());
        return;
    }

    TAutoPtr<TEvTxProcessing::TEvReadSetAck> ev = new TEvTxProcessing::TEvReadSetAck;

    ev->Record.SetSeqno(seqNo);
    ev->Record.SetTabletSource(rsInfo->From);
    ev->Record.SetTabletDest(rsInfo->To);
    ev->Record.SetTabletConsumer(rsInfo->Origin);
    ev->Record.SetTxId(rsInfo->TxId);

    SaveAck(ctx, ev);
}

void TOutReadSets::SaveAck(const TActorContext &ctx, TAutoPtr<TEvTxProcessing::TEvReadSetAck> ev) {
    ui64 seqno = ev->Record.GetSeqno();
    ui64 sender = ev->Record.GetTabletSource();
    ui64 dest = ev->Record.GetTabletDest();
    ui64 consumer = ev->Record.GetTabletConsumer();
    ui64 txId = ev->Record.GetTxId();

    LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
        "Receive RS Ack at %" PRIu64 " source %" PRIu64 " dest %" PRIu64 " consumer %" PRIu64 " txId %" PRIu64,
        Self->TabletID(), sender, dest, consumer, txId);

    ReadSetAcks.emplace_back(ev.Release());
    AckedSeqno.insert(seqno);

    if (CurrentReadSets.contains(seqno)) {
        TReadSetKey rsInfo(txId, Self->TabletID(), sender, dest);
        Y_VERIFY(CurrentReadSetInfos[rsInfo] == seqno);

        CurrentReadSets.erase(seqno);
        CurrentReadSetInfos.erase(rsInfo);
    }
}

void TOutReadSets::Cleanup(NIceDb::TNiceDb& db, const TActorContext& ctx) {
    using Schema = TDataShard::Schema;

    // Note that this code should be called only after no-more-reads to ensure we wont lost updates
    for (TIntrusivePtr<TEvTxProcessing::TEvReadSetAck>& event : ReadSetAcks) {
        TEvTxProcessing::TEvReadSetAck& ev = *event;
        ui64 seqno = ev.Record.GetSeqno();
        ui64 sender = ev.Record.GetTabletSource();
        ui64 dest = ev.Record.GetTabletDest();
        ui64 consumer = ev.Record.GetTabletConsumer();
        ui64 txId = ev.Record.GetTxId();

        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD,
            "Deleted RS at %" PRIu64 " source %" PRIu64 " dest %" PRIu64 " consumer %" PRIu64 " seqno %" PRIu64" txId %" PRIu64,
            Self->TabletID(), sender, dest, consumer, seqno, txId);

        db.Table<Schema::OutReadSets>().Key(seqno).Delete();
        Self->ResendReadSetPipeTracker.DetachTablet(seqno, ev.Record.GetTabletDest(), 0, ctx);
    }
    ReadSetAcks.clear();
    AckedSeqno.clear();

    UpdateMonCounter();
}

void TOutReadSets::ResendAll(const TActorContext& ctx) {
    TPendingPipeTrackerCommands pendingPipeTrackerCommands;
    for (const auto& rs : CurrentReadSets) {
        ui64 seqNo = rs.first;
        ui64 target = rs.second.To;
        Self->ResendReadSetQueue.Progress(rs.first, ctx);
        pendingPipeTrackerCommands.AttachTablet(seqNo, target);
    }
    pendingPipeTrackerCommands.Apply(Self->ResendReadSetPipeTracker, ctx);
}

bool TOutReadSets::ResendRS(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx, ui64 seqNo) {
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);
    if (AckedSeqno.contains(seqNo)) {
        // Do not resend if we've already got ACK back, but not applied it to DB
        // Also, it is a good place to actually apply ACK(s)

        txc.DB.NoMoreReadsForTx();
        Cleanup(db, ctx);
        return true;
    }

    auto rowset = db.Table<Schema::OutReadSets>().Key(seqNo).Select();
    if (!rowset.IsReady())
        return false;
    if (!rowset.IsValid())
        return true;

    ui64 step = rowset.GetValue<Schema::OutReadSets::Step>();
    ui64 txId = rowset.GetValue<Schema::OutReadSets::TxId>();
    ui64 from = rowset.GetValue<Schema::OutReadSets::From>();
    ui64 to = rowset.GetValue<Schema::OutReadSets::To>();
    TString body = rowset.GetValue<Schema::OutReadSets::Body>();

    txc.DB.NoMoreReadsForTx();

    Self->ResendReadSet(ctx, step, txId, from, to, body, seqNo);
    return true;
}

bool TOutReadSets::AddExpectation(ui64 target, ui64 step, ui64 txId) {
    auto res = Expectations[target].emplace(txId, step);
    return res.second;
}

bool TOutReadSets::RemoveExpectation(ui64 target, ui64 txId) {
    auto it = Expectations.find(target);
    if (it != Expectations.end()) {
        auto itTxId = it->second.find(txId);
        if (itTxId != it->second.end()) {
            it->second.erase(itTxId);
            if (it->second.empty()) {
                Expectations.erase(it);
            }
            return true;
        }
    }
    return false;
}

bool TOutReadSets::HasExpectations(ui64 target) {
    return Expectations.contains(target);
}

void TOutReadSets::ResendExpectations(ui64 target, const TActorContext& ctx) {
    auto it = Expectations.find(target);
    if (it != Expectations.end()) {
        for (const auto& pr : it->second) {
            Self->SendReadSetExpectation(ctx, pr.second, pr.first, Self->TabletID(), target);
        }
    }
}

THashMap<ui64, ui64> TOutReadSets::RemoveExpectations(ui64 target) {
    THashMap<ui64, ui64> result;
    auto it = Expectations.find(target);
    if (it != Expectations.end()) {
        result = std::move(it->second);
        Expectations.erase(it);
    }
    return result;
}

}}

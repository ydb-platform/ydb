#include "defs.h"

#include "datashard_outreadset.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

static constexpr size_t SmallReadSetCacheLimit = 8;

void TOutReadSets::UpdateMonCounter() const {
    Self->SetCounter(COUNTER_OUT_READSETS_IN_FLIGHT, CurrentReadSets.size());
}

bool TOutReadSets::LoadReadSets(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    CurrentReadSets.clear(); // For idempotency
    CurrentReadSetKeys.clear();

    // TODO[serxa]: this should be Range but it is not working right now
    auto rowset = db.Table<Schema::OutReadSets>().GreaterOrEqual(0).Select<
                                    Schema::OutReadSets::Seqno,
                                    Schema::OutReadSets::Step,
                                    Schema::OutReadSets::TxId,
                                    Schema::OutReadSets::Origin,
                                    Schema::OutReadSets::From,
                                    Schema::OutReadSets::To,
                                    Schema::OutReadSets::Body>();
    if (!rowset.IsReady())
        return false;
    while (!rowset.EndOfSet()) {
        ui64 seqNo = rowset.GetValue<Schema::OutReadSets::Seqno>();
        ui64 step = rowset.GetValue<Schema::OutReadSets::Step>();
        ui64 txId = rowset.GetValue<Schema::OutReadSets::TxId>();
        ui64 origin = rowset.GetValue<Schema::OutReadSets::Origin>();
        ui64 source = rowset.GetValue<Schema::OutReadSets::From>();
        ui64 target = rowset.GetValue<Schema::OutReadSets::To>();
        TString body = rowset.GetValue<Schema::OutReadSets::Body>();

        TReadSetInfo rsInfo;
        rsInfo.TxId = txId;
        rsInfo.Step = step;
        rsInfo.Origin = origin;
        rsInfo.From = source;
        rsInfo.To = target;
        // Cache it regardless of size, since we're going to send it soon
        rsInfo.Body = std::move(body);

        Y_ENSURE(!CurrentReadSets.contains(seqNo));
        Y_ENSURE(!CurrentReadSetKeys.contains(rsInfo));

        CurrentReadSetKeys[rsInfo] = seqNo;
        CurrentReadSets[seqNo] = std::move(rsInfo);

        if (!rowset.Next())
            return false;
    }

    UpdateMonCounter();
    return true;
}

void TOutReadSets::SaveReadSet(NIceDb::TNiceDb& db, ui64 seqNo, ui64 step, const TReadSetKey& rsKey, const TString& body) {
    using Schema = TDataShard::Schema;

    Y_ENSURE(!CurrentReadSets.contains(seqNo));
    Y_ENSURE(!CurrentReadSetKeys.contains(rsKey));

    TReadSetInfo rsInfo(rsKey);
    rsInfo.Step = step;
    if (body.size() <= SmallReadSetCacheLimit) {
        rsInfo.Body = body;
    }

    db.Table<Schema::OutReadSets>().Key(seqNo).Update(
        NIceDb::TUpdate<Schema::OutReadSets::Step>(rsInfo.Step),
        NIceDb::TUpdate<Schema::OutReadSets::TxId>(rsInfo.TxId),
        NIceDb::TUpdate<Schema::OutReadSets::Origin>(rsInfo.Origin),
        NIceDb::TUpdate<Schema::OutReadSets::From>(rsInfo.From),
        NIceDb::TUpdate<Schema::OutReadSets::To>(rsInfo.To),
        NIceDb::TUpdate<Schema::OutReadSets::Body>(body));

    CurrentReadSetKeys[rsKey] = seqNo;
    CurrentReadSets[seqNo] = std::move(rsInfo);

    UpdateMonCounter();
}

void TOutReadSets::RemoveReadSet(NIceDb::TNiceDb& db, ui64 seqNo) {
    using Schema = TDataShard::Schema;

    db.Table<Schema::OutReadSets>().Key(seqNo).Delete();

    auto it = CurrentReadSets.find(seqNo);
    if (it != CurrentReadSets.end()) {
        CurrentReadSetKeys.erase(it->second);
        CurrentReadSets.erase(it);
    }
}

TReadSetInfo TOutReadSets::ReplaceReadSet(NIceDb::TNiceDb& db, ui64 seqNo, const TString& body) {
    using Schema = TDataShard::Schema;

    auto it = CurrentReadSets.find(seqNo);
    if (it != CurrentReadSets.end()) {
        db.Table<Schema::OutReadSets>().Key(seqNo).Update(
            NIceDb::TUpdate<Schema::OutReadSets::Body>(body));
        if (body.size() <= SmallReadSetCacheLimit) {
            it->second.Body = body;
        } else {
            it->second.Body.reset();
        }
        return it->second;
    } else {
        return TReadSetInfo();
    }
}

void TOutReadSets::AckForDeletedDestination(ui64 tabletId, ui64 seqNo, const TActorContext &ctx) {
    const TReadSetKey* rsInfo = CurrentReadSets.FindPtr(seqNo);

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

    if (CurrentReadSets.contains(seqno)) {
        TReadSetKey rsKey(txId, Self->TabletID(), sender, dest);
        Y_ENSURE(CurrentReadSetKeys[rsKey] == seqno);

        CurrentReadSetKeys.erase(rsKey);
        CurrentReadSets.erase(seqno);
    }

    // We don't need to resend this readset anymore
    if (auto it = Self->PersistentTablets.find(dest); it != Self->PersistentTablets.end()) {
        it->second.OutReadSets.erase(seqno);
    }
}

void TOutReadSets::Cleanup(NIceDb::TNiceDb& db, const TActorContext& ctx) {
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

        RemoveReadSet(db, seqno);
    }
    ReadSetAcks.clear();

    UpdateMonCounter();
}

void TOutReadSets::ResendAll(const TActorContext& ctx) {
    for (const auto& rs : CurrentReadSets) {
        if (rs.second.OnHold) {
            continue;
        }
        ui64 seqNo = rs.first;
        Self->ResendReadSetQueue.Progress(seqNo, ctx);
    }
}

void TOutReadSets::HoldArbiterReadSets() {
    for (auto& rs : CurrentReadSets) {
        ui64 seqNo = rs.first;
        ui64 txId = rs.second.TxId;
        auto* info = Self->VolatileTxManager.FindByTxId(txId);
        if (info && info->IsArbiter && info->State != EVolatileTxState::Committed) {
            info->ArbiterReadSets.push_back(seqNo);
            info->IsArbiterOnHold = true;
            rs.second.OnHold = true;
        }
    }
}

void TOutReadSets::ReleaseOnHoldReadSets(const std::vector<ui64>& seqNos, const TActorContext& ctx) {
    for (ui64 seqNo : seqNos) {
        auto it = CurrentReadSets.find(seqNo);
        if (it != CurrentReadSets.end() && it->second.OnHold) {
            it->second.OnHold = false;
            Self->ResendReadSetQueue.Progress(seqNo, ctx);
        }
    }
}

bool TOutReadSets::ResendRS(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx, ui64 seqNo) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(txc.DB);

    auto* info = CurrentReadSets.FindPtr(seqNo);
    if (!info) {
        // Do not resend if we've already got ACK back, but not applied it to DB
        // Also, it is a good place to actually apply ACK(s)
        txc.DB.NoMoreReadsForTx();
        Cleanup(db, ctx);
        return true;
    }

    ui64 step = info->Step;
    ui64 txId = info->TxId;
    ui64 from = info->From;
    ui64 to = info->To;
    TString body;

    if (info->Body) {
        // We have readset body cached
        if (info->Body->size() <= SmallReadSetCacheLimit) {
            body = *info->Body;
        } else {
            // Don't keep it in memory while in transit
            body = std::move(*info->Body);
            info->Body.reset();
        }
    } else {
        auto rowset = db.Table<Schema::OutReadSets>().Key(seqNo).Select();
        if (!rowset.IsReady())
            return false;
        if (!rowset.IsValid())
            return true;
        body = rowset.GetValue<Schema::OutReadSets::Body>();
        if (body.size() <= SmallReadSetCacheLimit) {
            // Cache small readset body
            info->Body = body;
        }
    }

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

#include "datashard_impl.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NDataShard {

// Find and return parts that are no longer needed on the target datashard
class TDataShard::TTxInitiateBorrowedPartsReturn : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    THashMap<TLogoBlobID, NTabletFlatExecutor::TCompactedPartLoans> PartsToReturn;

public:
    TTxInitiateBorrowedPartsReturn(TDataShard* ds)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
    {}

    TTxType GetTxType() const override { return TXTYPE_INITIATE_BORROWED_PARTS_RETURN; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);
        // Prepare the list of parts to return
        PartsToReturn = *Self->Executor()->GetStats().CompactedPartLoans;
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        // group parts by owner tablet
        THashMap<ui64, TVector<TLogoBlobID>> perTabletParts;
        for (const auto& p : PartsToReturn) {
            ui64 ownerTabletId = p.second.Lender;
            TLogoBlobID partMeta = p.second.MetaInfoId;

            perTabletParts[ownerTabletId].push_back(partMeta);
        }

        for (const auto& batch : perTabletParts) {
            // open a pipe to the part owner and send part metadata batch
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " initiating parts " << batch.second <<  " return to " << batch.first);
            Self->LoanReturnTracker.ReturnLoan(batch.first, batch.second, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TDataShard::CreateTxInitiateBorrowedPartsReturn() {
    return new TTxInitiateBorrowedPartsReturn(this);
}

void TDataShard::CompletedLoansChanged(const TActorContext &ctx) {
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " CompletedLoansChanged");
    Y_ABORT_UNLESS(Executor()->GetStats().CompactedPartLoans);

    CheckInitiateBorrowedPartsReturn(ctx);
}

// Accept returned part on the source datashard
class TDataShard::TTxReturnBorrowedPart : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvReturnBorrowedPart::TPtr Ev;
    TVector<TLogoBlobID> PartMetaVec;
    ui64 FromTabletId;
public:
    TTxReturnBorrowedPart(TDataShard* ds, TEvDataShard::TEvReturnBorrowedPart::TPtr& ev)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_RETURN_BORROWED_PART; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        FromTabletId = Ev->Get()->Record.GetFromTabletId();
        for (ui32 i = 0; i < Ev->Get()->Record.PartMetadataSize(); ++i) {
            TLogoBlobID partMeta = LogoBlobIDFromLogoBlobID(Ev->Get()->Record.GetPartMetadata(i));
            PartMetaVec.push_back(partMeta);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " got returned parts " << PartMetaVec <<  " from " << FromTabletId);

            txc.Env.CleanupLoan(partMeta, FromTabletId);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        // Send Ack
        TActorId ackTo = Ev->Sender;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ack parts " << PartMetaVec << " return to tablet " << FromTabletId);

        ctx.Send(ackTo, new TEvDataShard::TEvReturnBorrowedPartAck(PartMetaVec), 0, Ev->Cookie);
        Self->CheckStateChange(ctx);
    }
};

// Forget the returned part on the target after source Ack from the source
class TDataShard::TTxReturnBorrowedPartAck : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvReturnBorrowedPartAck::TPtr Ev;
    TVector<TLogoBlobID> PartMetaVec;

public:
    TTxReturnBorrowedPartAck(TDataShard* ds, TEvDataShard::TEvReturnBorrowedPartAck::TPtr& ev)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_RETURN_BORROWED_PART_ACK; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        // Unregistered returned part
        for (ui32 i = 0; i < Ev->Get()->Record.PartMetadataSize(); ++i) {
            TLogoBlobID partMeta = LogoBlobIDFromLogoBlobID(Ev->Get()->Record.GetPartMetadata(i));
            PartMetaVec.push_back(partMeta);

            TLogoBlobID borrowId;
            txc.Env.ConfirmLoan(partMeta, borrowId);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " parts " << PartMetaVec << " return ack processed");
        for (const auto& partMeta : PartMetaVec) {
            Self->LoanReturnTracker.LoanDone(partMeta, ctx);
        }
        Self->CheckStateChange(ctx);
    }
};

void TDataShard::Handle(TEvDataShard::TEvReturnBorrowedPart::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxReturnBorrowedPart(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvReturnBorrowedPartAck::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxReturnBorrowedPartAck(this, ev), ctx);
}

bool TDataShard::HasSharedBlobs() const {
    const bool* hasSharedBlobsPtr = Executor()->GetStats().HasSharedBlobs;
    if (!hasSharedBlobsPtr) {
        Y_ABORT_UNLESS(Executor()->GetStats().IsFollower);
        return false;
    }
    return *hasSharedBlobsPtr;
}


// Switch to Offline state and notify the schemeshard to that it can initiate tablet deletion
class TDataShard::TTxGoOffline : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    explicit TTxGoOffline(TDataShard* ds)
        : NTabletFlatExecutor::TTransactionBase<TDataShard>(ds)
    {}

    TTxType GetTxType() const override { return TXTYPE_GO_OFFLINE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        if (Self->State == TShardState::Offline)
            return true;

        Y_ABORT_UNLESS(Self->State == TShardState::PreOffline, "Unexpected state %s tabletId %" PRIu64,
                 DatashardStateName(Self->State).data(), Self->TabletID());
        Y_ABORT_UNLESS(!Self->HasSharedBlobs(), "Cannot go offline while there are shared blobs at tablet %" PRIu64, Self->TabletID());
        Y_ABORT_UNLESS(!Self->TransQueue.TxInFly(), "Cannot go offline while there is a Tx in flight at tablet %" PRIu64, Self->TabletID());
        Y_ABORT_UNLESS(Self->OutReadSets.Empty(), "Cannot go offline while there is a non-Ack-ed readset at tablet %" PRIu64, Self->TabletID());
        Y_ABORT_UNLESS(Self->TransQueue.GetSchemaOperations().empty(), "Cannot go offline while there is a schema Tx in flight at tablet %" PRIu64, Self->TabletID());

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " Initiating switch from "
                    << DatashardStateName(Self->State) << " to Offline state");

        Self->PurgeTxTables(txc);

        NIceDb::TNiceDb db(txc.DB);

        Self->State = TShardState::Offline;
        Self->PersistSys(db, TDataShard::Schema::Sys_State, Self->State);
        Self->NotifyAllOverloadSubscribers();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        Self->ReportState(ctx, Self->State);
    }
};

void TDataShard::CheckInitiateBorrowedPartsReturn(const TActorContext &ctx) {
    if (!Executor()->GetStats().CompactedPartLoans->empty()) {
        Execute(CreateTxInitiateBorrowedPartsReturn(), ctx);
    }
}

void TDataShard::CheckStateChange(const TActorContext& ctx) {
    if (State == TShardState::PreOffline) {
        auto fnListTxIds = [](const auto& txMap) {
            TStringStream str;
            str << "[";
            for (const auto& it : txMap) {
                str << " " << it.first;
            }
            str << " ]";
            return str.Str();
        };

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " in PreOffline state"
                    << " HasSharedBobs: " << HasSharedBlobs()
                    << " SchemaOperations: " << fnListTxIds(TransQueue.GetSchemaOperations())
                    << " OutReadSets count: " << OutReadSets.CountReadSets()
                    << " ChangesQueue size: " << ChangesQueue.size()
                    << " ChangeExchangeSplit: " << ChangeExchangeSplitter.Done()
                    << " siblings to be activated: " << ChangeSenderActivator.Dump()
                    << " wait to activation from: " << JoinSeq(", ", ReceiveActivationsFrom));

        const bool hasSharedBlobs = HasSharedBlobs();
        const bool hasSchemaOps = !TransQueue.GetSchemaOperations().empty();
        const bool hasOutRs = !OutReadSets.Empty();
        const bool hasChangeRecords = !ChangesQueue.empty();
        const bool mustActivateOthers = !ChangeSenderActivator.AllAcked();

        if (!hasSharedBlobs && !hasSchemaOps && !hasOutRs && !hasChangeRecords && !mustActivateOthers) {
            Y_ABORT_UNLESS(!TxInFly());
            Execute(new TTxGoOffline(this), ctx);
        }
    }
}

}}

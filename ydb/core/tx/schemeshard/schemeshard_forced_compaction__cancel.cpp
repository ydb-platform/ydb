#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxCancel: public TRwTxBase {
    explicit TTxCancel(TSelf* self, TEvForcedCompaction::TEvCancelRequest::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev)
    {}

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        const auto& request = Request->Get()->Record;
        LOG_N("TForcedCompaction::TTxCancel DoExecute " << request.ShortDebugString());

        auto response = MakeHolder<TEvForcedCompaction::TEvCancelResponse>(request.GetTxId());
        TPath database = TPath::Resolve(request.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << request.GetDatabaseName() << " not found"
            );
        }
        const TPathId subdomainPathId = database.GetPathIdForDomain();
        
        auto compactionId = request.GetForcedCompactionId();
        const auto* forcedCompactionInfoPtr = Self->ForcedCompactions.FindPtr(compactionId);
        if (!forcedCompactionInfoPtr) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Forced compaction with id " << compactionId << " not found"
            );
        }
        auto& forcedCompactionInfo = *forcedCompactionInfoPtr->get();
        if (forcedCompactionInfo.SubdomainPathId != subdomainPathId) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Forced compaction with id " << compactionId << " not found in database " << request.GetDatabaseName()
            );
        }

        if (forcedCompactionInfo.IsFinished() ||
            forcedCompactionInfo.State == TForcedCompactionInfo::EState::Cancelling)
        {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Forced compaction with id " << compactionId << " has been finished or cancelling already"
            );
        }

        forcedCompactionInfo.State = TForcedCompactionInfo::EState::Cancelling;

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistForcedCompactionState(db, forcedCompactionInfo);

        // clean waiting shards
        auto* shardsQueue = Self->ForcedCompactionShardsByTable.FindPtr(forcedCompactionInfo.TablePathId);
        if (shardsQueue) {
            while (!shardsQueue->Empty()) {
                auto shardId = shardsQueue->Front();
                Self->InProgressForcedCompactionsByShard.erase(shardId);
                Self->PersistForcedCompactionDoneShard(db, shardId);
                shardsQueue->PopFront();
            }
            Self->ForcedCompactionShardsByTable.erase(forcedCompactionInfo.TablePathId);
            Self->ForcedCompactionTablesQueue.Remove(forcedCompactionInfo.TablePathId);
        }
        Self->InProgressForcedCompactionsByTable.erase(forcedCompactionInfo.TablePathId);

        // clean waiting in flight shards
        for (auto shardId : forcedCompactionInfo.ShardsInFlight) {
            if (Self->ForcedCompactionQueue) {
                Self->ForcedCompactionQueue->Remove(shardId);
            }
            Self->InProgressForcedCompactionsByShard.erase(shardId);
            Self->PersistForcedCompactionDoneShard(db, shardId);
        }
        forcedCompactionInfo.ShardsInFlight.clear();

        Self->CancellingForcedCompactions.emplace_back(*forcedCompactionInfoPtr, Request->Sender, request.GetTxId(), Request->Cookie);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxCancel DoComplete");
        SideEffects.ApplyOnComplete(Self, ctx);
        Self->ForcedCompactionProgressStartTime = ctx.Now();
        Self->Execute(Self->CreateTxProgressForcedCompaction());
    }

private:
    void Reply(
        THolder<TEvForcedCompaction::TEvCancelResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString())
    {
        auto& record = response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            auto& issue = *record.MutableIssues()->Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(errorMessage);

        }

        SideEffects.Send(Request->Sender, std::move(response), 0, Request->Cookie);
    }

private:
    TSideEffects SideEffects;
    TEvForcedCompaction::TEvCancelRequest::TPtr Request;
};

ITransaction* TSchemeShard::CreateTxCancelForcedCompaction(TEvForcedCompaction::TEvCancelRequest::TPtr& ev) {
    return new TForcedCompaction::TTxCancel(this, ev);
}

} // namespace NKikimr::NSchemeShard

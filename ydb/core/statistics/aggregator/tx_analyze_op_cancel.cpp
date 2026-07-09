#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeOpCancel : public TTxBase {
    TEvStatistics::TEvAnalyzeOpCancelRequest::TPtr Request;
    bool IsActive = false;

    TTxAnalyzeOpCancel(TSelf* self, TEvStatistics::TEvAnalyzeOpCancelRequest::TPtr ev)
        : TTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_OP_CANCEL; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& operationId = record.GetOperationId();
        const TString& dbName = record.GetDatabaseName();

        auto* op = Self->ForceTraversalOperation(operationId);
        if (!op || op->DatabaseName != dbName) {
            return true; // NOT_FOUND handled in Complete
        }

        if (IsTerminalAnalyzeState(op->State)) {
            return true; // idempotent
        }

        NIceDb::TNiceDb db(txc.DB);
        IsActive = (Self->ForceTraversalOperationId == operationId);

        if (!IsActive) {
            // Queued op: mark cancelled immediately
            Self->MarkForceTraversalOperationFinished(operationId,
                Ydb::Table::AnalyzeState::STATE_CANCELLED,
                ctx.Now(), db);
        }
        // Active op: handled via DispatchFinishTraversalTx in Complete
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& operationId = record.GetOperationId();
        const TString& dbName = record.GetDatabaseName();

        YDB_LOG_NOTICE("[AnalyzeOp] TTxAnalyzeOpCancel::Complete",
            {"tabletId", Self->TabletID()},
            {"opId", operationId.Quote()});

        auto response = MakeHolder<TEvStatistics::TEvAnalyzeOpCancelResponse>();
        auto& rec = response->Record;

        auto* op = Self->ForceTraversalOperation(operationId);
        if (!op || op->DatabaseName != dbName) {
            rec.SetStatus(Ydb::StatusIds::NOT_FOUND);
            auto& issue = *rec.AddIssues();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message("Operation not found");
            ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
            return;
        }

        rec.SetStatus(Ydb::StatusIds::SUCCESS);
        ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);

        if (IsActive) {
            Self->DispatchFinishTraversalTx(NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
        }
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeOpCancelRequest::TPtr& ev) {
    if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
        SendAnalyzeLongRunningOpDisabled<TEvStatistics::TEvAnalyzeOpCancelResponse>(ev->Sender, ev->Cookie);
        return;
    }
    Execute(new TTxAnalyzeOpCancel(this, std::move(ev)), TActivationContext::AsActorContext());
}

} // NKikimr::NStat

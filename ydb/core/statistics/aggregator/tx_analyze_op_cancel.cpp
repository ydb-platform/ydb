#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeOpCancel : public TTxBase {
    TEvStatistics::TEvAnalyzeOpCancelRequest::TPtr Request;
    TActorId AnalyzeRequester;
    bool Found = false;
    bool Cancelled = false;

    TTxAnalyzeOpCancel(TSelf* self, TEvStatistics::TEvAnalyzeOpCancelRequest::TPtr ev)
        : TTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_OP_CANCEL; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& operationId = record.GetOperationId();
        const TString& dbName = record.GetDatabaseName();

        YDB_LOG_DEBUG("[AnalyzeOp] TTxAnalyzeOpCancel::Execute",
            {"tabletId", Self->TabletID()},
            {"opId", operationId.Quote()},
            {"dbName", dbName});

        auto* op = Self->ForceTraversalOperation(operationId);
        if (!op || op->DatabaseName != dbName) {
            return true;
        }

        Found = true;
        if (IsTerminalAnalyzeState(op->State)) {
            return true; // idempotent
        }

        NIceDb::TNiceDb db(txc.DB);
        AnalyzeRequester = op->ReplyToActorId;
        Self->DiscardOperationTraversals(operationId, db);
        Self->MarkForceTraversalOperationFinished(operationId,
            Ydb::Table::AnalyzeState::STATE_CANCELLED, ctx.Now(), db);
        Cancelled = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& operationId = record.GetOperationId();

        YDB_LOG_NOTICE("[AnalyzeOp] TTxAnalyzeOpCancel::Complete",
            {"tabletId", Self->TabletID()},
            {"opId", operationId.Quote()});

        auto response = MakeHolder<TEvStatistics::TEvAnalyzeOpCancelResponse>();
        auto& rec = response->Record;

        if (!Found) {
            rec.SetStatus(Ydb::StatusIds::NOT_FOUND);
            auto& issue = *rec.AddIssues();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message("Operation not found");
            ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
            return;
        }

        rec.SetStatus(Ydb::StatusIds::SUCCESS);
        ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);

        if (AnalyzeRequester) {
            auto result = MakeHolder<TEvStatistics::TEvAnalyzeResponse>();
            result->Record.SetOperationId(operationId);
            result->Record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
            ctx.Send(AnalyzeRequester, result.Release());
        }
        if (Cancelled) {
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvScheduleForceTraversal());
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

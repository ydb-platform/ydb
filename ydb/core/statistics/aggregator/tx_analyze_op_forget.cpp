#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeOpForget : public TTxBase {
    TEvStatistics::TEvAnalyzeOpForgetRequest::TPtr Request;

    enum class EResult { NotFound, NonTerminal, Deleted };
    EResult Result = EResult::NotFound;

    TTxAnalyzeOpForget(TSelf* self, TEvStatistics::TEvAnalyzeOpForgetRequest::TPtr ev)
        : TTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_OP_FORGET; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        const TString& operationId = record.GetOperationId();
        const TString& dbName = record.GetDatabaseName();

        auto* op = Self->ForceTraversalOperation(operationId);
        if (!op || op->DatabaseName != dbName) {
            Result = EResult::NotFound;
            return true;
        }

        if (!IsTerminalAnalyzeState(op->State)) {
            Result = EResult::NonTerminal;
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->DeleteForceTraversalOperation(operationId, db);
        Result = EResult::Deleted;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        YDB_LOG_NOTICE("[AnalyzeOp] TTxAnalyzeOpForget::Complete",
            {"tabletId", Self->TabletID()},
            {"opId", record.GetOperationId().Quote()});

        auto response = MakeHolder<TEvStatistics::TEvAnalyzeOpForgetResponse>();
        auto& rec = response->Record;

        auto addError = [&rec](const TString& msg) {
            auto& issue = *rec.AddIssues();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(msg);
        };

        switch (Result) {
            case EResult::NotFound:
                rec.SetStatus(Ydb::StatusIds::NOT_FOUND);
                addError("Operation not found");
                break;
            case EResult::NonTerminal:
                rec.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
                addError("Cannot forget a non-terminal operation");
                break;
            case EResult::Deleted:
                rec.SetStatus(Ydb::StatusIds::SUCCESS);
                break;
        }

        ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeOpForgetRequest::TPtr& ev) {
    if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
        SendAnalyzeLongRunningOpDisabled<TEvStatistics::TEvAnalyzeOpForgetResponse>(ev->Sender, ev->Cookie);
        return;
    }
    Execute(new TTxAnalyzeOpForget(this, std::move(ev)), TActivationContext::AsActorContext());
}

} // NKikimr::NStat

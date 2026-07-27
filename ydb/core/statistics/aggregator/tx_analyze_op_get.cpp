#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeOpGet : public TTxBase {
    TEvStatistics::TEvAnalyzeOpGetRequest::TPtr Request;

    TTxAnalyzeOpGet(TSelf* self, TEvStatistics::TEvAnalyzeOpGetRequest::TPtr ev)
        : TTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_OP_GET; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& operationId = record.GetOperationId();
        const TString& dbName = record.GetDatabaseName();

        YDB_LOG_NOTICE("[AnalyzeOp] TTxAnalyzeOpGet::Complete",
            {"tabletId", Self->TabletID()},
            {"opId", operationId.Quote()});

        auto response = MakeHolder<TEvStatistics::TEvAnalyzeOpGetResponse>();
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
        Self->FillAnalyzeOperationProto(*op, *rec.MutableAnalyzeOperation());
        ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeOpGetRequest::TPtr& ev) {
    if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
        SendAnalyzeLongRunningOpDisabled<TEvStatistics::TEvAnalyzeOpGetResponse>(ev->Sender, ev->Cookie);
        return;
    }
    Execute(new TTxAnalyzeOpGet(this, std::move(ev)), TActivationContext::AsActorContext());
}

} // NKikimr::NStat

#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxGet: public TRwTxBase {
    explicit TTxGet(TSelf* self, TEvForcedCompaction::TEvGetRequest::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev)
    {}

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        LOG_N("TForcedCompaction::TTxGet DoExecute " << request.ShortDebugString());

        auto response = MakeHolder<TEvForcedCompaction::TEvGetResponse>();
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
        const auto& forcedCompactionInfo = *forcedCompactionInfoPtr->get();
        if (forcedCompactionInfo.SubdomainPathId != subdomainPathId) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Forced compaction with id " << compactionId << " not found in database " << request.GetDatabaseName()
            );
        }

        Self->FromForcedCompactionInfo(*response->Record.MutableForcedCompaction(), forcedCompactionInfo);

        Reply(std::move(response));

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxGet DoComplete");
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    void Reply(
        THolder<TEvForcedCompaction::TEvGetResponse> response,
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
    TEvForcedCompaction::TEvGetRequest::TPtr Request;
};

ITransaction* TSchemeShard::CreateTxGetForcedCompaction(TEvForcedCompaction::TEvGetRequest::TPtr& ev) {
    return new TForcedCompaction::TTxGet(this, ev);
}

} // namespace NKikimr::NSchemeShard

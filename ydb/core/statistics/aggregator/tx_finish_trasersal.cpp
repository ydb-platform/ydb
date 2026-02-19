#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    TString OperationId;
    TPathId PathId;
    TActorId ReplyToActorId;
    NKikimrStat::TEvAnalyzeResponse::EStatus Status;
    NYql::TIssues Issues;

    TTxFinishTraversal(
            TSelf* self,
            NKikimrStat::TEvAnalyzeResponse::EStatus status,
            NYql::TIssues issues = NYql::TIssues())
        : TTxBase(self)
        , OperationId(self->ForceTraversalOperationId)
        , PathId(self->TraversalPathId)
        , Status(status)
        , Issues(std::move(issues))
    {
        auto forceTraversal = Self->CurrentForceTraversalOperation();
        if (forceTraversal) {
            ReplyToActorId = forceTraversal->ReplyToActorId;
        }
    }

    TTxType GetTxType() const override { return TXTYPE_FINISH_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Execute");

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(
            db,
            /*finishAllForceTraversalTables=*/Status != NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete " <<
            Self->LastTraversalWasForceString() << " traversal for path " << PathId);

        if (!ReplyToActorId) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. No ActorId to send reply.");            
            return;
        }

        auto forceTraversalRemained = Self->ForceTraversalOperation(OperationId);       
        
        if (forceTraversalRemained) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. Don't send TEvAnalyzeResponse. " <<
                "There are pending operations, OperationId " << OperationId.Quote() << " , ActorId=" << ReplyToActorId);
        } else {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. " <<
                "Send TEvAnalyzeResponse, OperationId=" << OperationId.Quote() << ", ActorId=" << ReplyToActorId);
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(OperationId);
            response->Record.SetStatus(Status);
            for (const auto& issue : Issues) {
                NYql::IssueToMessage(issue, response->Record.AddIssues());
            }
            ctx.Send(ReplyToActorId, response.release());
        }
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(
        new TTxFinishTraversal(this, NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS),
        TActivationContext::AsActorContext());
}
void TStatisticsAggregator::Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr&) {
    NYql::TIssue error(TStringBuilder() << "Could not find table id: "
        << TraversalPathId.LocalPathId << ", deleted its statistics");
    Execute(
        new TTxFinishTraversal(this, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, {error}),
        TActivationContext::AsActorContext());
}
void TStatisticsAggregator::Handle(TEvStatistics::TEvFinishTraversal::TPtr& ev) {
    if (ev->Sender != AnalyzeActorId) {
        return;
    }

    using EStatus = TEvStatistics::TEvFinishTraversal::EStatus;
    switch (ev->Get()->Status) {
    case EStatus::Success:
        std::move(
            ev->Get()->Statistics.begin(), ev->Get()->Statistics.end(),
            std::back_inserter(StatisticsToSave));
        SaveStatisticsToTable();
        return;
    case EStatus::TableNotFound:
        DeleteStatisticsFromTable();
        return;
    case EStatus::InternalError:
        Execute(
            new TTxFinishTraversal(
                this, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, std::move(ev->Get()->Issues)),
            TActivationContext::AsActorContext());
        return;
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeCancel::TPtr& ev) {
    const auto& operationId = ev->Get()->Record.GetOperationId();
    if (operationId != ForceTraversalOperationId) {
        SA_LOG_N("Got unexpected TEvAnalyzeCancel with"
            << " operationId: " << operationId.Quote()
            << ", expected: " << ForceTraversalOperationId.Quote() << ", ignoring");
        return;
    }

    SA_LOG_D("Got TEvAnalyzeCancel, operationId: " << operationId.Quote());
    if (AnalyzeActorId) {
        Send(AnalyzeActorId, new TEvents::TEvPoison());
    }

    Execute(
        new TTxFinishTraversal(this, NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

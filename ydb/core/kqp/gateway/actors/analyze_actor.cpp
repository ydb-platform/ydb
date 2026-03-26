#include "analyze_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>


namespace NKikimr::NKqp {

enum {
    FirstRoundCookie = 0,
    SecondRoundCookie = 1,
};

using TNavigate = NSchemeCache::TSchemeCacheNavigate;

TAnalyzeActor::TAnalyzeActor(const TString& database, const TString& tablePath,
    const TVector<TString>& columns, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise)
    : Database(database)
    , TablePath(tablePath)
    , Columns(columns)
    , Promise(promise)
    , OperationId(UlidGen.Next(TActivationContext::Now()).ToBinary())
{}

void TAnalyzeActor::Bootstrap() {
    auto navigate = std::make_unique<TNavigate>();
    navigate->DatabaseName = Database;
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = SplitPath(TablePath);
    entry.Operation = TNavigate::EOp::OpTable;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    navigate->Cookie = FirstRoundCookie;

    Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));

    Become(&TAnalyzeActor::StateWork);
}

void TAnalyzeActor::Handle(NStat::TEvStatistics::TEvAnalyzeResponse::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);

    const auto& record = ev->Get()->Record;
    const TString operationId = record.GetOperationId();
    const auto status = record.GetStatus();

    NYql::IKikimrGateway::TGenericResult result;
    if (operationId != OperationId) {
        ALOG_CRIT(NKikimrServices::KQP_GATEWAY,
            "TAnalyzeActor, TEvAnalyzeResponse has operationId=" << operationId
            << " , but expected " << OperationId);
        result.SetStatus(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR);
        result.AddIssue(NYql::TIssue("ANALYZE failed: OperationId mismatch"));
    } else if (status != NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS) {
        ALOG_CRIT(NKikimrServices::KQP_GATEWAY,
            "TAnalyzeActor, TEvAnalyzeResponse has status=" << status);
        result.SetStatus(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR);
        NYql::TIssue error("Executing ANALYZE");
        for (const auto& issue : record.GetIssues()) {
            error.AddSubIssue(MakeIntrusive<NYql::TIssue>(NYql::IssueFromMessage(issue)));
        }
        result.AddIssue(error);
    } else {
        result.SetSuccess();
    }

    Promise.SetValue(std::move(result));
    this->Die(ctx);
}

void TAnalyzeActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());
    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();

    if (entry.Status != TNavigate::EStatus::Ok) {
        NYql::EYqlIssueCode error;
        switch (entry.Status) {
            case TNavigate::EStatus::PathErrorUnknown:
            case TNavigate::EStatus::RootUnknown:
            case TNavigate::EStatus::PathNotTable:
            case TNavigate::EStatus::TableCreationNotComplete:
                error = NYql::TIssuesIds::KIKIMR_SCHEME_ERROR;
            case TNavigate::EStatus::LookupError:
            case TNavigate::EStatus::RedirectLookupError:
                error = NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
            default:
                error = NYql::TIssuesIds::DEFAULT_ERROR;
        }
        Promise.SetValue(
            NYql::NCommon::ResultFromIssues<NYql::IKikimrGateway::TGenericResult>(
                error,
                TStringBuilder() << "Can't get statistics aggregator ID. " << entry.Status,
                {}
            )
        );
        this->Die(ctx);
        return;
    }

    if (navigate->Cookie == SecondRoundCookie) {
        if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
            SendStatisticsAggregatorAnalyze(entry, ctx);
        } else {
            Promise.SetValue(
                NYql::NCommon::ResultFromIssues<NYql::IKikimrGateway::TGenericResult>(
                    NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Can't get statistics aggregator ID.", {}
                )
            );
            this->Die(ctx);
        }
        return;
    }

    PathId = entry.TableId.PathId;

    auto& domainInfo = entry.DomainInfo;

    auto navigateDomainKey = [this] (TPathId domainKey) {
        auto navigate = std::make_unique<TNavigate>();
        navigate->DatabaseName = Database;
        auto& entry = navigate->ResultSet.emplace_back();
        entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        navigate->Cookie = SecondRoundCookie;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    };

    if (!domainInfo->IsServerless()) {
        if (domainInfo->Params.HasStatisticsAggregator()) {
            SendStatisticsAggregatorAnalyze(entry, ctx);
            return;
        }

        navigateDomainKey(domainInfo->DomainKey);
    } else {
        navigateDomainKey(domainInfo->ResourcesDomainKey);
    }
}

TDuration TAnalyzeActor::CalcBackoffTime() {
    ui32 backoffSlots = 1 << RetryCount;
    TDuration maxDuration = RetryInterval * backoffSlots;

    double uncertaintyRatio = std::max(std::min(UncertainRatio, 1.0), 0.0);
    double uncertaintyMultiplier = RandomNumber<double>() * uncertaintyRatio - uncertaintyRatio + 1.0;

    double durationMs = round(maxDuration.MilliSeconds() * uncertaintyMultiplier);
    durationMs = std::max(std::min(durationMs, MaxBackoffDurationMs), 0.0);
    return TDuration::MilliSeconds(durationMs);
}

void TAnalyzeActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev, ctx);

    if (RetryCount >= MaxRetryCount) {
        Promise.SetValue(
                NYql::NCommon::ResultFromError<NYql::IKikimrGateway::TGenericResult>(
                    YqlIssue(
                        {}, NYql::TIssuesIds::UNEXPECTED,
                        TStringBuilder() << "Can't establish connection with the Statistics Aggregator!"
                    )
                )
            );
        this->Die(ctx);
        return;
    }

    ++RetryCount;
    Schedule(CalcBackoffTime(), new TEvAnalyzePrivate::TEvAnalyzeRetry());
}

void TAnalyzeActor::Handle(TEvAnalyzePrivate::TEvAnalyzeRetry::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev, ctx);

    auto analyzeRequest = std::make_unique<NStat::TEvStatistics::TEvAnalyze>();
    analyzeRequest->Record = Request.Record;
    Send(
        MakePipePerNodeCacheID(EPipePerNodeCache::Leader),
        new TEvPipeCache::TEvForward(analyzeRequest.release(), StatisticsAggregatorId.value(), true),
        IEventHandle::FlagTrackDelivery
    );
}

void TAnalyzeActor::SendStatisticsAggregatorAnalyze(const TNavigate::TEntry& entry, const TActorContext& ctx) {
    Y_ABORT_UNLESS(entry.DomainInfo->Params.HasStatisticsAggregator());

    StatisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();

    auto& record = Request.Record;
    record.SetOperationId(OperationId);
    record.SetDatabase(Database);
    auto table = record.AddTables();

    PathId.ToProto(table->MutablePathId());

    THashMap<TString, ui32> tagByColumnName;
    for (const auto& [_, tableInfo]: entry.Columns) {
        tagByColumnName[TString(tableInfo.Name)] = tableInfo.Id;
    }

    for (const auto& columnName: Columns) {
        if (!tagByColumnName.contains(columnName)){
            Promise.SetValue(
                NYql::NCommon::ResultFromError<NYql::IKikimrGateway::TGenericResult>(
                    YqlIssue(
                        {}, NYql::TIssuesIds::UNEXPECTED,
                        TStringBuilder() << "No such column: " << columnName << " in the " << TablePath
                    )
                )
            );
            this->Die(ctx);
            return;
        }

        *table->MutableColumnTags()->Add() = tagByColumnName[columnName];
    }

    auto analyzeRequest = std::make_unique<NStat::TEvStatistics::TEvAnalyze>();
    analyzeRequest->Record = Request.Record;
    Send(
        MakePipePerNodeCacheID(EPipePerNodeCache::Leader),
        new TEvPipeCache::TEvForward(analyzeRequest.release(), entry.DomainInfo->Params.GetStatisticsAggregator(), true),
        IEventHandle::FlagTrackDelivery
    );
}

void TAnalyzeActor::Handle(TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
    ALOG_NOTICE(
        NKikimrServices::KQP_GATEWAY,
        "got TEvAbortExecution, issues: " << ev->Get()->GetIssues().ToOneLineString());

    if (StatisticsAggregatorId) {
        // We already sent the request to StatisticsAggregator, make a best-effort attempt to cancel it.
        auto cancelRequest = std::make_unique<NStat::TEvStatistics::TEvAnalyzeCancel>();
        cancelRequest->Record.SetOperationId(OperationId);
        Send(
            MakePipePerNodeCacheID(EPipePerNodeCache::Leader),
            new TEvPipeCache::TEvForward(cancelRequest.release(), StatisticsAggregatorId.value(), false));
    }

    Promise.SetValue(
        NYql::NCommon::ResultFromError<NYql::IKikimrGateway::TGenericResult>(ev->Get()->GetIssues()));
    this->Die(ctx);
}

void TAnalyzeActor::HandleUnexpectedEvent(ui32 typeRewrite) {
    ALOG_CRIT(
        NKikimrServices::KQP_GATEWAY,
        "TAnalyzeActor, unexpected event, request type: " << typeRewrite);

    Promise.SetValue(
        NYql::NCommon::ResultFromError<NYql::IKikimrGateway::TGenericResult>(
            YqlIssue(
                {}, NYql::TIssuesIds::UNEXPECTED,
                TStringBuilder() << "Unexpected event: " << typeRewrite
            )
        )
    );

    this->PassAway();
}

void TAnalyzeActor::PassAway() {
    Send(MakePipePerNodeCacheID(EPipePerNodeCache::Leader), new TEvPipeCache::TEvUnlink(0));
    TActorBootstrapped::PassAway();
}

}// end of NKikimr::NKqp

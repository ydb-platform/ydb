#include "analyze_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <cmath>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_GATEWAY


namespace NKikimr::NKqp {

enum {
    FirstRoundCookie = 0,
    SecondRoundCookie = 1,
};

using TNavigate = NSchemeCache::TSchemeCacheNavigate;

TAnalyzeActor::TAnalyzeActor(const TString& database, const TString& tablePath,
    const TVector<TString>& columns, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise,
    double sampleRate)
    : Database(database)
    , TablePath(tablePath)
    , Columns(columns)
    , SampleRate(sampleRate)
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
        YDB_LOG_CRIT("TAnalyzeActor received unexpected operation id in TEvAnalyzeResponse",
            {"operationId", operationId},
            {"expectedOperationId", OperationId});
        result.SetStatus(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR);
        result.AddIssue(NYql::TIssue("ANALYZE failed: OperationId mismatch"));
    } else if (status != NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        YDB_LOG_WARN("TAnalyzeActor, TEvAnalyzeResponse has",
            {"status", status},
            {"operationId", OperationId.Quote()},
            {"database", Database},
            {"tablePath", TablePath},
            {"pathId", PathId},
            {"statisticsAggregatorId", StatisticsAggregatorId.value_or(0)},
            {"issues", issues.ToOneLineString()});
        result.SetStatus(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR);
        NYql::TIssue error("Executing ANALYZE");
        for (const auto& issue : issues) {
            error.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
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
            SendStatisticsAggregatorAnalyze(entry);
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

    if (SampleRate != 1.0 && !entry.ColumnTableInfo) {
        Promise.SetValue(NYql::NCommon::ResultFromIssues<NYql::IKikimrGateway::TGenericResult>(
            NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
            "ANALYZE SAMPLE is supported only for column tables", {}));
        this->Die(ctx);
        return;
    }

    PathId = entry.TableId.PathId;

    if (!BuildAnalyzeRequest(entry, ctx)) {
        return;
    }

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
            SendStatisticsAggregatorAnalyze(entry);
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
    if (ev->Get()->TabletId != StatisticsAggregatorId) {
        return;
    }

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

void TAnalyzeActor::SendAnalyzeRequest() {
    auto analyzeRequest = std::make_unique<NStat::TEvStatistics::TEvAnalyze>();
    analyzeRequest->Record = Request.Record;
    Send(
        MakePipePerNodeCacheID(EPipePerNodeCache::Leader),
        new TEvPipeCache::TEvForward(analyzeRequest.release(), StatisticsAggregatorId.value(), true),
        IEventHandle::FlagTrackDelivery
    );
}

bool TAnalyzeActor::BuildAnalyzeRequest(const TNavigate::TEntry& entry, const TActorContext& ctx) {
    auto& record = Request.Record;
    record.SetOperationId(OperationId);
    record.SetDatabase(Database);
    auto table = record.AddTables();

    PathId.ToProto(table->MutablePathId());
    table->SetPath(TablePath);
    if (SampleRate != 1.0) {
        table->SetSampleRate(SampleRate);
    }

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
            return false;
        }

        *table->MutableColumnTags()->Add() = tagByColumnName[columnName];
    }
    return true;
}

void TAnalyzeActor::SendStatisticsAggregatorAnalyze(const TNavigate::TEntry& entry) {
    Y_ABORT_UNLESS(entry.DomainInfo->Params.HasStatisticsAggregator());

    StatisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();
    SendAnalyzeRequest();
}

void TAnalyzeActor::Handle(TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_NOTICE("Got TEvAbortExecution",
        {"issues", ev->Get()->GetIssues().ToOneLineString()});

    // ANALYZE is a long-running operation: tying its lifetime to the calling query
    // would mean a session timeout silently cancels work the user may want to keep.
    // Match BUILD_INDEX / EXPORT / IMPORT / COMPACTION: the calling actor dies, but
    // the SA traversal continues. The user can poll via `ydb operation get analyze`
    // and explicitly cancel via `ydb operation cancel analyze`; orphans are bounded
    // by the SA-side deadline in tx_analyze_deadline.cpp.
    Promise.SetValue(
        NYql::NCommon::ResultFromError<NYql::IKikimrGateway::TGenericResult>(ev->Get()->GetIssues()));
    this->Die(ctx);
}

void TAnalyzeActor::HandleUnexpectedEvent(ui32 typeRewrite) {
    YDB_LOG_CRIT("TAnalyzeActor, unexpected event, request",
        {"type", typeRewrite});

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

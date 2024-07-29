#include "analyze_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>


namespace NKikimr::NKqp {

enum {
    FirstRoundCookie = 0,
    SecondRoundCookie = 1,
};

using TNavigate = NSchemeCache::TSchemeCacheNavigate;

void TAnalyzeActor::Bootstrap() {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    auto navigate = std::make_unique<TNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = SplitPath(TablePath);
    entry.Operation = TNavigate::EOp::OpTable;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    navigate->Cookie = FirstRoundCookie;

    Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));

    Become(&TAnalyzeActor::StateWork);
}

void TAnalyzeActor::Handle(NStat::TEvStatistics::TEvAnalyzeResponse::TPtr& ev, const TActorContext& ctx) {
    NYql::IKikimrGateway::TGenericResult result;
    result.SetSuccess();
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
        }

        this->Die(ctx);
        return;
    }
    
    PathId = entry.TableId.PathId;

    auto& domainInfo = entry.DomainInfo;

    auto navigateDomainKey = [this] (TPathId domainKey) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = std::make_unique<TNavigate>();
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

void TAnalyzeActor::SendStatisticsAggregatorAnalyze(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TActorContext& ctx) {
    Y_ASSERT(entry.DomainInfo->Params.HasStatisticsAggregator());

    auto analyzeRequest = std::make_unique<NStat::TEvStatistics::TEvAnalyze>();
    auto& record = analyzeRequest->Record;
    auto table = record.AddTables();
    
    PathIdFromPathId(PathId, table->MutablePathId());


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

    Send(
        MakePipePerNodeCacheID(false),
        new TEvPipeCache::TEvForward(analyzeRequest.release(), entry.DomainInfo->Params.GetStatisticsAggregator(), true),
        IEventHandle::FlagTrackDelivery
    );
}

void TAnalyzeActor::HandleUnexpectedEvent(ui32 typeRewrite) {
    ALOG_CRIT(
        NKikimrServices::KQP_GATEWAY, 
        "TAnalyzeActor, unexpected event, request type: " << typeRewrite;
    );
        
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

}// end of NKikimr::NKqp

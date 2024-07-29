#include "http_request.h"


#include <ydb/core/base/path.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NStat {

THttpRequest::THttpRequest(EType type, const TString& path, TActorId replyToActorId)
    : Type(type)
    , Path(path)
    , ReplyToActorId(replyToActorId)
{}    

void THttpRequest::Bootstrap() {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    auto navigate = std::make_unique<TNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = SplitPath(Path);
    entry.Operation = TNavigate::EOp::OpTable;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    navigate->Cookie = FirstRoundCookie;

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));

    Become(&THttpRequest::StateWork);
}

void THttpRequest::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());
    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();

    if (navigate->Cookie == SecondRoundCookie) {
        if (entry.Status != TNavigate::EStatus::Ok) {
            HttpReply("Internal error");
            return;
        }
        if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
            StatisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();
        }
        ResolveSuccess();
        return;
    }

    if (entry.Status != TNavigate::EStatus::Ok) {
        switch (entry.Status) {
        case TNavigate::EStatus::PathErrorUnknown:
            HttpReply("Path does not exist");
            return;
        case TNavigate::EStatus::PathNotPath:
            HttpReply("Invalid path");
            return;
        case TNavigate::EStatus::PathNotTable:
            HttpReply("Path is not a table");
            return;
        default:
            HttpReply("Internal error");
            return;
        }
    }

    PathId = entry.TableId.PathId;

    auto& domainInfo = entry.DomainInfo;
    ui64 aggregatorId = 0;
    if (domainInfo->Params.HasStatisticsAggregator()) {
        aggregatorId = domainInfo->Params.GetStatisticsAggregator();
    }
    bool isServerless = domainInfo->IsServerless();
    TPathId domainKey = domainInfo->DomainKey;
    TPathId resourcesDomainKey = domainInfo->ResourcesDomainKey;

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

    if (!isServerless) {
        if (aggregatorId) {
            StatisticsAggregatorId = aggregatorId;
            ResolveSuccess();
        } else {
            navigateDomainKey(domainKey);
        }
    } else {
        navigateDomainKey(resourcesDomainKey);
    }
}

void THttpRequest::Handle(TEvStatistics::TEvAnalyzeStatusResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    switch (record.GetStatus()) {
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_UNSPECIFIED:
        HttpReply("Status is unspecified");
        break;
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION:
        HttpReply("No scan operation");
        break;
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED:
        HttpReply("Scan is enqueued");
        break;
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS:
        HttpReply("Scan is in progress");
        break;
    }
}

void THttpRequest::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    HttpReply("Delivery problem");
}

void THttpRequest::ResolveSuccess() {
    if (StatisticsAggregatorId == 0) {
        HttpReply("No statistics aggregator");
        return;
    }

    if (Type == ANALYZE) {
        auto analyze = std::make_unique<TEvStatistics::TEvAnalyze>();
        auto& record = analyze->Record;
        PathIdFromPathId(PathId, record.AddTables()->MutablePathId());

        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(analyze.release(), StatisticsAggregatorId, true));

        HttpReply("Scan sent");
    } else {
        auto getStatus = std::make_unique<TEvStatistics::TEvAnalyzeStatus>();
        auto& record = getStatus->Record;
        PathIdFromPathId(PathId, record.MutablePathId());

        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(getStatus.release(), StatisticsAggregatorId, true));
    }
}

void THttpRequest::HttpReply(const TString& msg) {
    Send(ReplyToActorId, new NMon::TEvHttpInfoRes(msg));
    PassAway();
}

void THttpRequest::PassAway() {
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBase::PassAway();
}



} // NStat
} // NKikimr
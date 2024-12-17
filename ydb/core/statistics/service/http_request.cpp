#include "http_request.h"

#include <ydb/core/statistics/service/service.h>
#include <ydb/core/base/path.h>
#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/statistics/database/database.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr {
namespace NStat {

static constexpr ui64 FirstRoundCookie = 1;
static constexpr ui64 SecondRoundCookie = 2;

THttpRequest::THttpRequest(ERequestType requestType, const std::unordered_map<EParamType, TString>& params, const TActorId& replyToActorId)
    : RequestType(requestType)
    , Params(params)
    , ReplyToActorId(replyToActorId)
{}

void THttpRequest::Bootstrap() {
    auto navigate = std::make_unique<TNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = SplitPath(Params[EParamType::PATH]);
    entry.Operation = TNavigate::EOp::OpTable;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.ShowPrivatePath = true;
    navigate->Cookie = FirstRoundCookie;

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));

    Become(&THttpRequest::StateWork);
}

void THttpRequest::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());
    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);

    const auto& entry = navigate->ResultSet.front();

    if (navigate->Cookie == SecondRoundCookie) {
        if (entry.Status != TNavigate::EStatus::Ok) {
            HttpReply("Internal error");
            return;
        }

        DoRequest(entry);
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

    if (RequestType == ERequestType::COUNT_MIN_SKETCH_PROBE) {
        DoRequest(entry);
        return;
    }

    auto navigateDomainKey = [this] (TPathId domainKey) {
        auto navigate = std::make_unique<TNavigate>();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        navigate->Cookie = SecondRoundCookie;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    };

    const auto& domainInfo = entry.DomainInfo;

    if (domainInfo->IsServerless()) {
        navigateDomainKey(domainInfo->ResourcesDomainKey);
        return;
    }

    if (!domainInfo->Params.HasStatisticsAggregator()) {
        navigateDomainKey(domainInfo->DomainKey);
        return;
    }

    DoRequest(entry);
}

void THttpRequest::Handle(TEvStatistics::TEvAnalyzeStatusResponse::TPtr& ev) {
    const auto& record = ev->Get()->Record;

    switch (record.GetStatus()) {
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_UNSPECIFIED:
        HttpReply("Status is unspecified");
        break;
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION:
        HttpReply("No analyze operation");
        break;
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED:
        HttpReply("Analyze is enqueued");
        break;
    case NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS:
        HttpReply("Analyze is in progress");
        break;
    }
}

void THttpRequest::Handle(TEvStatistics::TEvGetStatisticsResult::TPtr& ev) {
    const auto msg = ev->Get();

    if (!msg->Success
            || msg->StatResponses.empty() || !msg->StatResponses[0].Success
            || msg->StatResponses[0].CountMinSketch.CountMin == nullptr) {
        HttpReply("Error occurred while loading statistics.");
        return;
    }

    const auto typeId = static_cast<NScheme::TTypeId>(ev->Cookie);
    const NScheme::TTypeInfo typeInfo(typeId);
    const TStringBuf value(Params[EParamType::CELL_VALUE]);
    TMemoryPool pool(64);

    TCell cell;
    TString error;
    if (!NFormats::MakeCell(cell, value, typeInfo, pool, error)) {
        HttpReply("Cell value parsing error: " + error);
        return;
    }

    const auto countMinSketch = msg->StatResponses[0].CountMinSketch.CountMin.get();
    const auto probe = countMinSketch->Probe(cell.Data(), cell.Size());
    HttpReply(Params[EParamType::PATH] + "[" + Params[EParamType::COLUMN_NAME] + "]=" + std::to_string(probe));
}

void THttpRequest::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    HttpReply("Delivery problem");
}

void THttpRequest::DoRequest(const TNavigate::TEntry& entry) {
    switch (RequestType) {
        case ERequestType::ANALYZE:
            DoAnalyze(entry);
            return;
        case ERequestType::STATUS:
            DoStatus(entry);
            return;
        case ERequestType::COUNT_MIN_SKETCH_PROBE:
            DoCountMinSketchProbe(entry);
            return;
    }
}

void THttpRequest::DoAnalyze(const TNavigate::TEntry& entry) {
    if (!entry.DomainInfo->Params.HasStatisticsAggregator()) {
        HttpReply("No statistics aggregator");
        return;
    }

    const auto statisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();
    const auto operationId = TULIDGenerator().Next(TActivationContext::Now());

    auto analyze = std::make_unique<TEvStatistics::TEvAnalyze>();
    auto& record = analyze->Record;
    record.SetOperationId(operationId.ToBinary());

    const auto& pathId = entry.TableId.PathId;
    PathIdFromPathId(pathId, record.AddTables()->MutablePathId());

    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(analyze.release(), statisticsAggregatorId, true));
    HttpReply("Analyze sent. OperationId: " + operationId.ToString());
}

void THttpRequest::DoStatus(const TNavigate::TEntry& entry) {
    if (!entry.DomainInfo->Params.HasStatisticsAggregator()) {
        HttpReply("No statistics aggregator");
        return;
    }

    const auto statisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();

    const auto& operationIdParam = Params[EParamType::OPERATION_ID];
    TULID operationId;

    if (operationIdParam.empty() || !operationId.ParseString(operationIdParam)) {
        HttpReply(TString("Wrong OperationId: ") + (operationIdParam.empty() ? "Empty" : operationIdParam));
    }

    auto status = std::make_unique<TEvStatistics::TEvAnalyzeStatus>();
    auto& record = status->Record;
    record.SetOperationId(operationId.ToBinary());

    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(status.release(), statisticsAggregatorId, true));
}

void THttpRequest::DoCountMinSketchProbe(const TNavigate::TEntry& entry) {
    const auto& columnName = Params[EParamType::COLUMN_NAME];
    if (columnName.empty()) {
        HttpReply("Column is not set");
        return;
    }

    if (Params[EParamType::CELL_VALUE].empty()) {
        HttpReply("Value is not set");
        return;
    }

    for (const auto& [_, tableInfo]: entry.Columns) {
        if (tableInfo.Name == columnName) {
            auto request = std::make_unique<TEvStatistics::TEvGetStatistics>();
            request->StatType = EStatType::COUNT_MIN_SKETCH;
            TRequest req;
            req.PathId = entry.TableId.PathId;
            req.ColumnTag = tableInfo.Id;
            request->StatRequests.emplace_back(std::move(req));

            const auto typeId = tableInfo.PType.GetTypeId();
            const auto statService = MakeStatServiceID(SelfId().NodeId());
            Send(statService, request.release(), 0, typeId);
            return;
        }
    }

    HttpReply("Column not found");
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

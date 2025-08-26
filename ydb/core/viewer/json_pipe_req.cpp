#include "json_pipe_req.h"
#include "log.h"
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NViewer {

template<typename T>
class TCachedResponseState {
public:
    static constexpr TDuration CACHE_STALE_PERIOD = TDuration::Seconds(30); // when we start to update cache
    static constexpr TDuration CACHE_VALID_PERIOD = TDuration::Seconds(60); // when we are ok to get data from the cache

    struct TResult {
        std::shared_ptr<T> Data;
        TDuration Age;
    };

    std::shared_ptr<T> GetFromCache(TDuration& cachedDataMaxAge, bool& needToUpdate) {
        std::lock_guard lock(Mutex);
        if (IsCacheValid()) {
            LastAccessed = Now();
            cachedDataMaxAge = std::max(cachedDataMaxAge, GetAge());
            if (GetAge() >= CACHE_STALE_PERIOD && !UpdatingNow) {
                needToUpdate = UpdatingNow = true;
            }
            return Response;
        }
        return {};
    }

    void UpdateCache(std::shared_ptr<T> response) {
        std::lock_guard lock(Mutex);
        Response = std::move(response);
        LastModified = Now();
        UpdatingNow = false;
    }

    void ClearOld() {
        if (IsCacheOld()) {
            std::lock_guard lock(Mutex);
            if (IsCacheOld()) {
                Response.reset();
                UpdatingNow = false; // very simple solution ... we don't retry, we just wait and forget
            }
        }
    }

protected:
    static TInstant Now() {
        return TActivationContext::Now();
    }

    TDuration GetAge() const {
        return Now() - LastModified;
    }

    bool IsCacheFresh() const {
        return GetAge() < CACHE_VALID_PERIOD;
    }

    bool IsCacheValid() const {
        return Response && IsCacheFresh();
    }

    bool IsCacheOld() const {
        return Response && !IsCacheFresh();
    }

    std::mutex Mutex;
    std::shared_ptr<T> Response;
    TInstant LastModified;
    TInstant LastAccessed;
    bool UpdatingNow = false;
};

struct TViewerSharedCacheState {
    TCachedResponseState<NSysView::TEvSysView::TEvGetGroupsResponse> StorageGroups;
    TCachedResponseState<NSysView::TEvSysView::TEvGetStoragePoolsResponse> StoragePools;
    TCachedResponseState<NSysView::TEvSysView::TEvGetVSlotsResponse> StorageVSlots;
    TCachedResponseState<NSysView::TEvSysView::TEvGetPDisksResponse> StoragePDisks;
    TCachedResponseState<NSysView::TEvSysView::TEvGetStorageStatsResponse> StorageStats;

    void ClearOld() {
        StorageGroups.ClearOld();
        StoragePools.ClearOld();
        StorageVSlots.ClearOld();
        StoragePDisks.ClearOld();
        StorageStats.ClearOld();
    }
};

std::shared_ptr<TViewerSharedCacheState> IViewer::CreateSharedCacheState() {
    return std::make_shared<TViewerSharedCacheState>();
}

void IViewer::DeleteOldSharedCacheData() {
    SharedCacheState->ClearOld();
}

void IViewer::UpdateSharedCacheData(std::unique_ptr<TEvViewer::TEvUpdateSharedCacheTabletResponse> ev) {
    std::visit(TOverloaded{
        [&](const std::shared_ptr<NSysView::TEvSysView::TEvGetGroupsResponse>& r) { SharedCacheState->StorageGroups.UpdateCache(r); },
        [&](const std::shared_ptr<NSysView::TEvSysView::TEvGetStoragePoolsResponse>& r) { SharedCacheState->StoragePools.UpdateCache(r); },
        [&](const std::shared_ptr<NSysView::TEvSysView::TEvGetVSlotsResponse>& r) { SharedCacheState->StorageVSlots.UpdateCache(r); },
        [&](const std::shared_ptr<NSysView::TEvSysView::TEvGetPDisksResponse>& r) { SharedCacheState->StoragePDisks.UpdateCache(r); },
        [&](const std::shared_ptr<NSysView::TEvSysView::TEvGetStorageStatsResponse>& r) { SharedCacheState->StorageStats.UpdateCache(r); },
    }, ev->Response);
}

void TViewerPipeClient::UpdateSharedCacheTablet(TTabletId tabletId, std::unique_ptr<IEventBase> request) {
    Send(MakeViewerID(SelfId().NodeId()), new TEvViewer::TEvUpdateSharedCacheTabletRequest(tabletId, std::move(request)));
}

NTabletPipe::TClientConfig TViewerPipeClient::GetPipeClientConfig() {
    NTabletPipe::TClientConfig clientConfig;
    if (WithRetry) {
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    }
    return clientConfig;
}

TViewerPipeClient::~TViewerPipeClient() = default;

TViewerPipeClient::TViewerPipeClient() = default;

TViewerPipeClient::TViewerPipeClient(NWilson::TTraceId traceId) {
    if (traceId) {
        Span = {TComponentTracingLevels::THttp::TopLevel, std::move(traceId), "viewer", NWilson::EFlags::AUTO_END};
    }
}

void TViewerPipeClient::BuildParamsFromJson(TStringBuf data) {
    NJson::TJsonValue jsonData;
    if (NJson::ReadJsonTree(data, &jsonData)) {
        if (jsonData.IsMap()) {
            for (const auto& [key, value] : jsonData.GetMap()) {
                switch (value.GetType()) {
                    case NJson::EJsonValueType::JSON_STRING:
                    case NJson::EJsonValueType::JSON_INTEGER:
                    case NJson::EJsonValueType::JSON_UINTEGER:
                    case NJson::EJsonValueType::JSON_DOUBLE:
                    case NJson::EJsonValueType::JSON_BOOLEAN:
                        Params.InsertUnescaped(key, value.GetStringRobust());
                        break;
                    case NJson::EJsonValueType::JSON_ARRAY:
                        for (const auto& item : value.GetArray()) {
                            Params.InsertUnescaped(key, item.GetStringRobust());
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        PostData = std::move(jsonData);
    }
}

void TViewerPipeClient::BuildParamsFromFormData(TStringBuf data) {
    for (const auto& [key, value] : TCgiParameters(data)) {
        Params.InsertUnescaped(key, value);
    }
}

void TViewerPipeClient::SetupTracing(const TString& handlerName) {
    auto request = GetRequest();
    NWilson::TTraceId traceId;
    TString traceparent = request.GetHeader("traceparent");
    if (traceparent) {
        traceId = NWilson::TTraceId::FromTraceparentHeader(traceparent, TComponentTracingLevels::ProductionVerbose);
    }
    TString wantTrace = request.GetHeader("X-Want-Trace");
    TString traceVerbosity = request.GetHeader("X-Trace-Verbosity");
    TString traceTTL = request.GetHeader("X-Trace-TTL");
    if (!traceId && (FromStringWithDefault<bool>(wantTrace) || !traceVerbosity.empty() || !traceTTL.empty())) {
        ui8 verbosity = TComponentTracingLevels::ProductionVerbose;
        if (traceVerbosity) {
            verbosity = FromStringWithDefault<ui8>(traceVerbosity, verbosity);
            verbosity = std::min(verbosity, NWilson::TTraceId::MAX_VERBOSITY);
        }
        ui32 ttl = Max<ui32>();
        if (traceTTL) {
            ttl = FromStringWithDefault<ui32>(traceTTL, ttl);
            ttl = std::min(ttl, NWilson::TTraceId::MAX_TIME_TO_LIVE);
        }
        traceId = NWilson::TTraceId::NewTraceId(verbosity, ttl);
    }
    if (traceId) {
        Span = {TComponentTracingLevels::THttp::TopLevel, std::move(traceId), handlerName ? "http " + handlerName : "http viewer", NWilson::EFlags::AUTO_END};
        TString uri = request.GetUri();
        Span.Attribute("request_type", TString(TStringBuf(uri).Before('?')));
        Span.Attribute("request_params", TString(TStringBuf(uri).After('?')));
    }
}

TViewerPipeClient::TViewerPipeClient(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev, const TString& handlerName)
    : Viewer(viewer)
    , Event(ev)
{
    Params = Event->Get()->Request.GetParams();
    if (NHttp::Trim(Event->Get()->Request.GetHeader("Content-Type").Before(';'), ' ') == "application/json") {
        BuildParamsFromJson(Event->Get()->Request.GetPostContent());
    }
    if (NHttp::Trim(Event->Get()->Request.GetHeader("Content-Type").Before(';'), ' ') == "application/x-www-form-urlencoded") {
        BuildParamsFromFormData(Event->Get()->Request.GetPostContent());
    }
    InitConfig(Params);
    SetupTracing(handlerName);
}

TViewerPipeClient::TViewerPipeClient(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, const TString& handlerName)
    : Viewer(viewer)
    , HttpEvent(ev)
{
    Params = TCgiParameters(HttpEvent->Get()->Request->URL.After('?'));
    NHttp::THeaders headers(HttpEvent->Get()->Request->Headers);
    if (NHttp::Trim(headers.Get("Content-Type").Before(';'), ' ') == "application/json") {
        BuildParamsFromJson(HttpEvent->Get()->Request->Body);
    }
    if (NHttp::Trim(headers.Get("Content-Type").Before(';'), ' ') == "application/x-www-form-urlencoded") {
        BuildParamsFromFormData(HttpEvent->Get()->Request->Body);
    }
    InitConfig(Params);
    SetupTracing(handlerName);
}

TActorId TViewerPipeClient::ConnectTabletPipe(NNodeWhiteboard::TTabletId tabletId) {
    TPipeInfo& pipeInfo = PipeInfo[tabletId];
    if (!pipeInfo.PipeClient) {
        auto pipe = NTabletPipe::CreateClient(SelfId(), tabletId, GetPipeClientConfig());
        pipeInfo.PipeClient = RegisterWithSameMailbox(pipe);
    }
    pipeInfo.Requests++;
    return pipeInfo.PipeClient;
}

void TViewerPipeClient::SendEvent(std::unique_ptr<IEventHandle> event) {
    if (DelayedRequests.empty() && DataRequests < MaxRequestsInFlight) {
        TActivationContext::Send(event.release());
        ++DataRequests;
    } else {
        DelayedRequests.push_back({
            .Event = std::move(event),
        });
    }
}

void TViewerPipeClient::SendRequest(TActorId recipient, IEventBase* ev, ui32 flags, ui64 cookie, NWilson::TTraceId traceId) {
    SendEvent(std::make_unique<IEventHandle>(recipient, SelfId(), ev, flags, cookie, nullptr /*forwardOnNondelivery*/, std::move(traceId)));
}

void TViewerPipeClient::SendRequestToPipe(TActorId pipe, IEventBase* ev, ui64 cookie, NWilson::TTraceId traceId) {
    std::unique_ptr<IEventHandle> event = std::make_unique<IEventHandle>(pipe, SelfId(), ev, 0 /*flags*/, cookie, nullptr /*forwardOnNondelivery*/, std::move(traceId));
    event->Rewrite(TEvTabletPipe::EvSend, pipe);
    SendEvent(std::move(event));
}

void TViewerPipeClient::SendDelayedRequests() {
    while (!DelayedRequests.empty() && DataRequests < MaxRequestsInFlight) {
        auto& request(DelayedRequests.front());
        TActivationContext::Send(request.Event.release());
        ++DataRequests;
        DelayedRequests.pop_front();
    }
}

TPathId TViewerPipeClient::GetPathId(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev) {
    if (ev.Request->ResultSet.size() == 1) {
        if (ev.Request->ResultSet.begin()->Self) {
            const auto& info = ev.Request->ResultSet.begin()->Self->Info;
            return TPathId(info.GetSchemeshardId(), info.GetPathId());
        }
        if (ev.Request->ResultSet.begin()->TableId) {
            return ev.Request->ResultSet.begin()->TableId.PathId;
        }
    }
    return {};
}

TString TViewerPipeClient::GetPath(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev) {
    if (ev.Request->ResultSet.size() == 1) {
        return CanonizePath(ev.Request->ResultSet.begin()->Path);
    }
    return {};
}

TPathId TViewerPipeClient::GetPathId(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    return GetPathId(*ev->Get());
}

TString TViewerPipeClient::GetPath(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    return GetPath(*ev->Get());
}

bool TViewerPipeClient::IsSuccess(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev) {
    return (ev.Request->ResultSet.size() > 0) && (std::find_if(ev.Request->ResultSet.begin(), ev.Request->ResultSet.end(),
        [](const auto& entry) {
            return entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        }) != ev.Request->ResultSet.end());
}

TString TViewerPipeClient::GetError(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev) {
    if (ev.Request->ResultSet.size() == 0) {
        return "empty response";
    }
    for (const auto& entry : ev.Request->ResultSet) {
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    return "Ok";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                    return "Unknown";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                    return "RootUnknown";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    return "PathErrorUnknown";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                    return "PathNotTable";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
                    return "PathNotPath";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                    return "TableCreationNotComplete";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                    return "LookupError";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    return "RedirectLookupError";
                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                    return "AccessDenied";
                default:
                    return ::ToString(static_cast<int>(ev.Request->ResultSet.begin()->Status));
            }
        }
    }
    return "no error";
}

bool TViewerPipeClient::IsSuccess(const TEvStateStorage::TEvBoardInfo& ev) {
    return ev.Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok;
}

TString TViewerPipeClient::GetError(const TEvStateStorage::TEvBoardInfo& ev) {
    switch (ev.Status) {
        case TEvStateStorage::TEvBoardInfo::EStatus::Unknown:
            return "Unknown";
        case TEvStateStorage::TEvBoardInfo::EStatus::Ok:
            return "Ok";
        case TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable:
            return "NotAvailable";
        default:
            return ::ToString(static_cast<int>(ev.Status));
    }
}

bool TViewerPipeClient::IsSuccess(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult& ev) {
    return ev.GetRecord().GetStatus() == NKikimrScheme::EStatus::StatusSuccess;
}

TString TViewerPipeClient::GetError(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult& ev) {
    if (ev.GetRecord().HasReason()) {
        return ev.GetRecord().GetReason();
    }
    return NKikimrScheme::EStatus_Name(ev.GetRecord().GetStatus());
}

bool TViewerPipeClient::IsSuccess(const TEvTxUserProxy::TEvProposeTransactionStatus& ev) {
    switch (ev.Record.GetStatus()) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress:
            return true;
    }
    return false;
}

TString TViewerPipeClient::GetError(const TEvTxUserProxy::TEvProposeTransactionStatus& ev) {
    return TStringBuilder() << ev.Record.GetStatus();
}

bool TViewerPipeClient::IsSuccess(const NKqp::TEvGetScriptExecutionOperationResponse& ev) {
    return ev.Status == Ydb::StatusIds::SUCCESS;
}

TString TViewerPipeClient::GetError(const NKqp::TEvGetScriptExecutionOperationResponse& ev) {
    return Ydb::StatusIds_StatusCode_Name(ev.Status);
}

void TViewerPipeClient::RequestHiveDomainStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
    request->Record.SetReturnFollowers(Followers);
    request->Record.SetReturnMetrics(Metrics);
    SendRequestToPipe(pipeClient, request.Release(), hiveId);
}

void TViewerPipeClient::RequestHiveNodeStats(NNodeWhiteboard::TTabletId hiveId, TPathId pathId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveNodeStats> request = MakeHolder<TEvHive::TEvRequestHiveNodeStats>();
    request->Record.SetReturnMetrics(Metrics);
    if (pathId != TPathId()) {
        request->Record.SetReturnExtendedTabletInfo(true);
        request->Record.SetFilterTabletsBySchemeShardId(pathId.OwnerId);
        request->Record.SetFilterTabletsByPathId(pathId.LocalPathId);
    }
    SendRequestToPipe(pipeClient, request.Release(), hiveId);
}

void TViewerPipeClient::RequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
    SendRequestToPipe(pipeClient, request.Release(), hiveId);
}

TViewerPipeClient::TRequestResponse<TEvViewer::TEvViewerResponse> TViewerPipeClient::MakeViewerRequest(TNodeId nodeId, TEvViewer::TEvViewerRequest* ev, ui32 flags) {
    TActorId viewerServiceId = MakeViewerID(nodeId);
    TRequestResponse<TEvViewer::TEvViewerResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
    if (response.Span) {
        response.Span.Attribute("target_node_id", nodeId);
        TStringBuilder askFor;
        askFor << ev->Record.GetLocation().NodeIdSize() << " nodes (";
        for (size_t i = 0; i < std::min<size_t>(ev->Record.GetLocation().NodeIdSize(), 16); ++i) {
            if (i) {
                askFor << ", ";
            }
            askFor << ev->Record.GetLocation().GetNodeId(i);
        }
        if (ev->Record.GetLocation().NodeIdSize() > 16) {
            askFor << ", ...";
        }
        askFor << ")";
        response.Span.Attribute("ask_for", askFor);
        switch (ev->Record.Request_case()) {
            case NKikimrViewer::TEvViewerRequest::kTabletRequest:
                response.Span.Attribute("request_type", "TabletRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kSystemRequest:
                response.Span.Attribute("request_type", "SystemRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kPDiskRequest:
                response.Span.Attribute("request_type", "PDiskRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kVDiskRequest:
                response.Span.Attribute("request_type", "VDiskRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kNodeRequest:
                response.Span.Attribute("request_type", "NodeRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kQueryRequest:
                response.Span.Attribute("request_type", "QueryRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kRenderRequest:
                response.Span.Attribute("request_type", "RenderRequest");
                break;
            case NKikimrViewer::TEvViewerRequest::kAutocompleteRequest:
                response.Span.Attribute("request_type", "AutocompleteRequest");
                break;
            default:
                response.Span.Attribute("request_type", ::ToString(static_cast<int>(ev->Record.Request_case())));
                break;
        }
    }
    SendRequest(viewerServiceId, ev, flags, nodeId, response.Span.GetTraceId());
    return response;
}

TViewerPipeClient::TRequestResponse<TEvHive::TEvResponseHiveDomainStats> TViewerPipeClient::MakeRequestHiveDomainStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
    request->Record.SetReturnFollowers(Followers);
    request->Record.SetReturnMetrics(Metrics);
    auto response = MakeRequestToPipe<TEvHive::TEvResponseHiveDomainStats>(pipeClient, request.Release(), hiveId);
    if (response.Span) {
        auto hive_id = "#" + ::ToString(hiveId);
        response.Span.Attribute("hive_id", hive_id);
    }
    return response;
}

TViewerPipeClient::TRequestResponse<TEvHive::TEvResponseHiveStorageStats> TViewerPipeClient::MakeRequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
    auto response = MakeRequestToPipe<TEvHive::TEvResponseHiveStorageStats>(pipeClient, request.Release(), hiveId);
    if (response.Span) {
        auto hive_id = "#" + ::ToString(hiveId);
        response.Span.Attribute("hive_id", hive_id);
    }
    return response;
}

TViewerPipeClient::TRequestResponse<TEvHive::TEvResponseHiveNodeStats> TViewerPipeClient::MakeRequestHiveNodeStats(TTabletId hiveId, TEvHive::TEvRequestHiveNodeStats* request) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    auto response = MakeRequestToPipe<TEvHive::TEvResponseHiveNodeStats>(pipeClient, request, hiveId);
    if (response.Span) {
        response.Span.Attribute("hive_id", TStringBuilder() << '#' << hiveId);
        if (request->Record.GetFilterTabletsBySchemeShardId()) {
            response.Span.Attribute("schemeshard_id", TStringBuilder() << '#' << request->Record.GetFilterTabletsBySchemeShardId());
        }
        if (request->Record.GetFilterTabletsByPathId()) {
            response.Span.Attribute("path_id", TStringBuilder() << '#' << request->Record.GetFilterTabletsByPathId());
        }
        if (request->Record.HasFilterTabletsByObjectDomain()) {
            response.Span.Attribute("object_domain", TStringBuilder() << TSubDomainKey(request->Record.GetFilterTabletsByObjectDomain()));
        }
    }
    return response;
}

TViewerPipeClient::TRequestResponse<TEvViewer::TEvViewerResponse> TViewerPipeClient::MakeRequestViewer(TNodeId nodeId, TEvViewer::TEvViewerRequest* request, ui32 flags) {
    auto requestType = request->Record.GetRequestCase();
    auto response = MakeRequest<TEvViewer::TEvViewerResponse>(MakeViewerID(nodeId), request, flags, nodeId);
    if (response.Span) {
        TString requestTypeString;
        switch (requestType) {
            case NKikimrViewer::TEvViewerRequest::kTabletRequest:
                requestTypeString = "TabletRequest";
                break;
            case NKikimrViewer::TEvViewerRequest::kSystemRequest:
                requestTypeString = "SystemRequest";
                break;
            case NKikimrViewer::TEvViewerRequest::kQueryRequest:
                requestTypeString = "QueryRequest";
                break;
            case NKikimrViewer::TEvViewerRequest::kRenderRequest:
                requestTypeString = "RenderRequest";
                break;
            case NKikimrViewer::TEvViewerRequest::kAutocompleteRequest:
                requestTypeString = "AutocompleteRequest";
                break;
            default:
                requestTypeString = ::ToString(static_cast<int>(requestType));
                break;
        }
        response.Span.Attribute("request_type", requestTypeString);
    }
    return response;
}

void TViewerPipeClient::RequestConsoleListTenants() {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClient::TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> TViewerPipeClient::MakeRequestConsoleListTenants() {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
    return MakeRequestToPipe<NConsole::TEvConsole::TEvListTenantsResponse>(pipeClient, request.Release());
}

TViewerPipeClient::TRequestResponse<NConsole::TEvConsole::TEvGetNodeConfigResponse> TViewerPipeClient::MakeRequestConsoleNodeConfigByTenant(TString tenant, ui64 cookie) {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    auto request = MakeHolder<NConsole::TEvConsole::TEvGetNodeConfigRequest>();
    request->Record.MutableNode()->SetTenant(tenant);
    request->Record.AddItemKinds(static_cast<ui32>(NKikimrConsole::TConfigItem::FeatureFlagsItem));
    return MakeRequestToPipe<NConsole::TEvConsole::TEvGetNodeConfigResponse>(pipeClient, request.Release(), cookie);
}

TViewerPipeClient::TRequestResponse<NConsole::TEvConsole::TEvGetAllConfigsResponse> TViewerPipeClient::MakeRequestConsoleGetAllConfigs() {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    return MakeRequestToPipe<NConsole::TEvConsole::TEvGetAllConfigsResponse>(pipeClient, new NConsole::TEvConsole::TEvGetAllConfigsRequest());
}

void TViewerPipeClient::RequestConsoleGetTenantStatus(const TString& path) {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
    request->Record.MutableRequest()->set_path(path);
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClient::TRequestResponse<NConsole::TEvConsole::TEvGetTenantStatusResponse> TViewerPipeClient::MakeRequestConsoleGetTenantStatus(const TString& path) {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
    request->Record.MutableRequest()->set_path(path);
    auto response = MakeRequestToPipe<NConsole::TEvConsole::TEvGetTenantStatusResponse>(pipeClient, request.Release());
    if (response.Span) {
        response.Span.Attribute("path", path);
    }
    return response;
}

void TViewerPipeClient::RequestBSControllerConfig() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClient::RequestBSControllerConfigWithStoragePools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClient::TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> TViewerPipeClient::MakeRequestBSControllerConfigWithStoragePools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
    return MakeRequestToPipe<TEvBlobStorage::TEvControllerConfigResponse>(pipeClient, request.Release());
}

void TViewerPipeClient::RequestBSControllerInfo() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvRequestControllerInfo> request = MakeHolder<TEvBlobStorage::TEvRequestControllerInfo>();
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClient::RequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClient::TRequestResponse<TEvBlobStorage::TEvControllerSelectGroupsResult> TViewerPipeClient::MakeRequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request, ui64 cookie) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    return MakeRequestToPipe<TEvBlobStorage::TEvControllerSelectGroupsResult>(pipeClient, request.Release(), cookie);
}

TViewerPipeClient::TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> TViewerPipeClient::RequestBSControllerPDiskRestart(ui32 nodeId, ui32 pdiskId, bool force) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* restartPDisk = request->Record.MutableRequest()->AddCommand()->MutableRestartPDisk();
    restartPDisk->MutableTargetPDiskId()->SetNodeId(nodeId);
    restartPDisk->MutableTargetPDiskId()->SetPDiskId(pdiskId);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    auto response = MakeRequestToTablet<TEvBlobStorage::TEvControllerConfigResponse>(GetBSControllerId(), request.Release());
    if (response.Span) {
        response.Span.Attribute("node_id", nodeId);
        response.Span.Attribute("pdisk_id", pdiskId);
        response.Span.Attribute("force", force);
    }
    return response;
}

TViewerPipeClient::TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> TViewerPipeClient::RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* evictVDisk = request->Record.MutableRequest()->AddCommand()->MutableReassignGroupDisk();
    evictVDisk->SetGroupId(groupId);
    evictVDisk->SetGroupGeneration(groupGeneration);
    evictVDisk->SetFailRealmIdx(failRealmIdx);
    evictVDisk->SetFailDomainIdx(failDomainIdx);
    evictVDisk->SetVDiskIdx(vdiskIdx);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    auto response = MakeRequestToTablet<TEvBlobStorage::TEvControllerConfigResponse>(GetBSControllerId(), request.Release());
    if (response.Span) {
        response.Span.Attribute("vdisk_id", TStringBuilder() << groupId << '-' << groupGeneration << '-' << failRealmIdx << '-' << failDomainIdx << '-' << vdiskIdx);
    }
    return response;
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClient::RequestBSControllerPDiskInfo(ui32 nodeId, ui32 pdiskId) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
    request->Record.SetInclusiveFrom(true);
    request->Record.SetInclusiveTo(true);
    request->Record.MutableFrom()->SetNodeId(nodeId);
    request->Record.MutableFrom()->SetPDiskId(pdiskId);
    request->Record.MutableTo()->SetNodeId(nodeId);
    request->Record.MutableTo()->SetPDiskId(pdiskId);
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClient::RequestBSControllerVDiskInfo(ui32 nodeId, ui32 pdiskId) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
    request->Record.SetInclusiveFrom(true);
    request->Record.SetInclusiveTo(true);
    request->Record.MutableFrom()->SetNodeId(nodeId);
    request->Record.MutableFrom()->SetPDiskId(pdiskId);
    request->Record.MutableFrom()->SetVSlotId(0);
    request->Record.MutableTo()->SetNodeId(nodeId);
    request->Record.MutableTo()->SetPDiskId(pdiskId);
    request->Record.MutableTo()->SetVSlotId(std::numeric_limits<ui32>::max());
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> TViewerPipeClient::RequestBSControllerGroups() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>();
    auto response = MakeRequestToPipe<NSysView::TEvSysView::TEvGetGroupsResponse>(pipeClient, request.release());
    return response;
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> TViewerPipeClient::MakeCachedRequestBSControllerGroups() {
    if (UseCache && Viewer) {
        bool needUpdate = false;
        auto cachedData = Viewer->SharedCacheState->StorageGroups.GetFromCache(CachedDataMaxAge, needUpdate);
        if (needUpdate) {
            UpdateSharedCacheTablet(GetBSControllerId(), std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>());
        }
        if (cachedData) {
            return cachedData;
        }
    }
    return RequestBSControllerGroups();
}
TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> TViewerPipeClient::RequestBSControllerPools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetStoragePoolsResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> TViewerPipeClient::MakeCachedRequestBSControllerPools() {
    if (UseCache && Viewer) {
        bool needUpdate = false;
        auto cachedData = Viewer->SharedCacheState->StoragePools.GetFromCache(CachedDataMaxAge, needUpdate);
        if (needUpdate) {
            UpdateSharedCacheTablet(GetBSControllerId(), std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>());
        }
        if (cachedData) {
            return cachedData;
        }
    }
    return RequestBSControllerPools();
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClient::RequestBSControllerVSlots() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClient::MakeCachedRequestBSControllerVSlots() {
    if (UseCache && Viewer) {
        bool needUpdate = false;
        auto cachedData = Viewer->SharedCacheState->StorageVSlots.GetFromCache(CachedDataMaxAge, needUpdate);
        if (needUpdate) {
            UpdateSharedCacheTablet(GetBSControllerId(), std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>());
        }
        if (cachedData) {
            return cachedData;
        }
    }
    return RequestBSControllerVSlots();
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClient::RequestBSControllerPDisks() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClient::MakeCachedRequestBSControllerPDisks() {
    if (UseCache && Viewer) {
        bool needUpdate = false;
        auto cachedData = Viewer->SharedCacheState->StoragePDisks.GetFromCache(CachedDataMaxAge, needUpdate);
        if (needUpdate) {
            UpdateSharedCacheTablet(GetBSControllerId(), std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>());
        }
        if (cachedData) {
            return cachedData;
        }
    }
    return RequestBSControllerPDisks();
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetStorageStatsResponse> TViewerPipeClient::RequestBSControllerStorageStats() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetStorageStatsResponse>(pipeClient, new NSysView::TEvSysView::TEvGetStorageStatsRequest());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetStorageStatsResponse> TViewerPipeClient::MakeCachedRequestBSControllerStorageStats() {
    if (UseCache && Viewer) {
        bool needUpdate = false;
        auto cachedData = Viewer->SharedCacheState->StorageStats.GetFromCache(CachedDataMaxAge, needUpdate);
        if (needUpdate) {
            UpdateSharedCacheTablet(GetBSControllerId(), std::make_unique<NSysView::TEvSysView::TEvGetStorageStatsRequest>());
        }
        if (cachedData) {
            return cachedData;
        }
    }
    return RequestBSControllerStorageStats();
}

TViewerPipeClient::TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> TViewerPipeClient::RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* updateDriveStatus = request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
    updateDriveStatus->CopyFrom(driveStatus);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    return MakeRequestToTablet<TEvBlobStorage::TEvControllerConfigResponse>(GetBSControllerId(), request.Release());
}

THolder<NSchemeCache::TSchemeCacheNavigate> TViewerPipeClient::SchemeCacheNavigateRequestBuilder (
        NSchemeCache::TSchemeCacheNavigate::TEntry&& entry
) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    entry.RedirectRequired = false;
    entry.ShowPrivatePath = true;
    if (entry.Operation == NSchemeCache::TSchemeCacheNavigate::OpUnknown)
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(std::move(entry));
    return request;
}

void TViewerPipeClient::RequestSchemeCacheNavigate(const TString& path) {
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);

    auto request = SchemeCacheNavigateRequestBuilder(std::move(entry));
    SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void TViewerPipeClient::RequestSchemeCacheNavigate(const TPathId& pathId) {
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    auto request = SchemeCacheNavigateRequestBuilder(std::move(entry));
    SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

TViewerPipeClient::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClient::MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0 /*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("path", path);
    }
    return response;
}

TViewerPipeClient::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClient::MakeRequestSchemeCacheNavigate(TPathId pathId, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0 /*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("path_id", pathId.ToString());
    }
    return response;
}

TViewerPipeClient::TRequestResponse<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>
    TViewerPipeClient::MakeRequestSchemeShardDescribe(TTabletId schemeShardId, const TString& path, const NKikimrSchemeOp::TDescribeOptions& options, ui64 cookie) {
    auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>();
    request->Record.SetSchemeshardId(schemeShardId);
    request->Record.SetPath(path);
    request->Record.MutableOptions()->CopyFrom(options);
    auto response = MakeRequestToTablet<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(schemeShardId, request.release(), cookie);
    if (response.Span) {
        response.Span.Attribute("path", path);
    }
    return response;
}

TViewerPipeClient::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClient::MakeRequestSchemeCacheNavigateWithToken(
        const TString& path, ui32 access, ui64 cookie
) {
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.Access = access;
    entry.SyncVersion = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpList;
    auto request = SchemeCacheNavigateRequestBuilder(std::move(entry));

    if (!Event->Get()->UserToken.empty())
         request->UserToken = new NACLib::TUserToken(Event->Get()->UserToken);

    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
            MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0 /*flags*/, cookie
    );
    if (response.Span) {
        response.Span.Attribute("path", path);
    }
    return response;
}

void TViewerPipeClient::RequestTxProxyDescribe(const TString& path, const NKikimrSchemeOp::TDescribeOptions& options) {
    THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->CopyFrom(options);
    if (Event && !Event->Get()->UserToken.empty()) {
        request->Record.SetUserToken(Event->Get()->UserToken);
    }
    if (HttpEvent && !HttpEvent->Get()->UserToken.empty()) {
        request->Record.SetUserToken(HttpEvent->Get()->UserToken);
    }
    request->Record.MutableDescribePath()->MutableOptions()->CopyFrom(options);
    SendRequest(MakeTxProxyID(), request.Release());
}

void TViewerPipeClient::RequestStateStorageEndpointsLookup(const TString& path) {
    RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(path),
                                                   SelfId(),
                                                   EBoardLookupMode::Second));
    ++DataRequests;
}

TViewerPipeClient::TRequestResponse<TEvStateStorage::TEvBoardInfo> TViewerPipeClient::MakeRequestStateStorageEndpointsLookup(const TString& path, ui64 cookie) {
    TRequestResponse<TEvStateStorage::TEvBoardInfo> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, "BoardLookupActor-Endpoints"));
    RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(path),
                                                   SelfId(),
                                                   EBoardLookupMode::Second, {}, cookie));
    if (response.Span) {
        response.Span.Attribute("path", path);
    }
    ++DataRequests;
    return response;
}

TViewerPipeClient::TRequestResponse<TEvStateStorage::TEvBoardInfo> TViewerPipeClient::MakeRequestStateStorageMetadataCacheEndpointsLookup(const TString& path, ui64 cookie) {
    TRequestResponse<TEvStateStorage::TEvBoardInfo> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, "BoardLookupActor-MetadataCache"));
    RegisterWithSameMailbox(CreateBoardLookupActor(MakeDatabaseMetadataCacheBoardPath(path),
                                                   SelfId(),
                                                   EBoardLookupMode::Second, {}, cookie));
    if (response.Span) {
        response.Span.Attribute("path", path);
    }
    ++DataRequests;
    return response;
}

std::vector<TNodeId> TViewerPipeClient::GetNodesFromBoardReply(const TEvStateStorage::TEvBoardInfo& ev) {
    std::vector<TNodeId> databaseNodes;
    if (ev.Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
        for (const auto& [actorId, infoEntry] : ev.InfoEntries) {
            databaseNodes.emplace_back(actorId.NodeId());
        }
    }
    std::sort(databaseNodes.begin(), databaseNodes.end());
    databaseNodes.erase(std::unique(databaseNodes.begin(), databaseNodes.end()), databaseNodes.end());
    return databaseNodes;
}

std::vector<TNodeId> TViewerPipeClient::GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
    return GetNodesFromBoardReply(*ev->Get());
}

std::vector<TNodeId> TViewerPipeClient::GetDatabaseNodes() {
    if (DatabaseBoardInfoResponse && DatabaseBoardInfoResponse->IsOk()) {
        return GetNodesFromBoardReply(DatabaseBoardInfoResponse->GetRef());
    } else if (ResourceBoardInfoResponse && ResourceBoardInfoResponse->IsOk()) {
        return GetNodesFromBoardReply(ResourceBoardInfoResponse->GetRef());
    }
    return {0};
}

bool TViewerPipeClient::IsDatabaseRequest() {
    return DatabaseBoardInfoResponse || ResourceBoardInfoResponse;
}

void TViewerPipeClient::InitConfig(const TCgiParameters& params) {
    Followers = FromStringWithDefault(params.Get("followers"), Followers);
    Metrics = FromStringWithDefault(params.Get("metrics"), Metrics);
    WithRetry = FromStringWithDefault(params.Get("with_retry"), WithRetry);
    MaxRequestsInFlight = FromStringWithDefault(params.Get("max_requests_in_flight"), MaxRequestsInFlight);
    Database = params.Get("database");
    if (!Database) {
        Database = params.Get("tenant");
    }
    Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
    JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
    JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
    if (FromStringWithDefault<bool>(params.Get("enums"), true)) {
        Proto2JsonConfig.EnumMode = TProto2JsonConfig::EnumValueMode::EnumName;
    }
    if (!FromStringWithDefault<bool>(params.Get("ui64"), false)) {
        Proto2JsonConfig.StringifyNumbers = TProto2JsonConfig::EStringifyNumbersMode::StringifyInt64Always;
        Proto2JsonConfig.StringifyNumbersRepeated = TProto2JsonConfig::EStringifyNumbersMode::StringifyInt64Always;
    }
    Proto2JsonConfig.MapAsObject = true;
    Proto2JsonConfig.ConvertAny = true;
    Proto2JsonConfig.WriteNanAsString = true;
    Proto2JsonConfig.DoubleNDigits = 17;
    Proto2JsonConfig.FloatNDigits = 9;
    Timeout = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("timeout"), Timeout.MilliSeconds()));
    UseCache = FromStringWithDefault<bool>(params.Get("use_cache"), UseCache);
}

void TViewerPipeClient::InitConfig(const TRequestSettings& settings) {
    Followers = settings.Followers;
    Metrics = settings.Metrics;
    WithRetry = settings.WithRetry;
}

void TViewerPipeClient::ClosePipes() {
    for (const auto& [tabletId, pipeInfo] : PipeInfo) {
        if (pipeInfo.PipeClient) {
            NTabletPipe::CloseClient(SelfId(), pipeInfo.PipeClient);
        }
    }
    PipeInfo.clear();
}

i32 TViewerPipeClient::FailPipeConnect(NNodeWhiteboard::TTabletId tabletId) {
    auto itPipeInfo = PipeInfo.find(tabletId);
    if (itPipeInfo != PipeInfo.end()) {
        i32 requests = itPipeInfo->second.Requests;
        NTabletPipe::CloseClient(SelfId(), itPipeInfo->second.PipeClient);
        PipeInfo.erase(itPipeInfo);
        return requests;
    }
    return 0;
}

TRequestState TViewerPipeClient::GetRequest() const {
    if (Event) {
        return {Event->Get(), Span.GetTraceId()};
    } else if (HttpEvent) {
        return {HttpEvent->Get(), Span.GetTraceId()};
    }
    return {};
}

void TViewerPipeClient::ReplyAndPassAway(TString data, const TString& error) {
    if (!ReplySent) {
        if (Event) {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        } else if (HttpEvent) {
            auto response = HttpEvent->Get()->Request->CreateResponseString(data);
            Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response.Release()));
        }
        ReplySent = true;
        if (error) {
            Error = error;
        }
        if (Error.empty()) {
            TStringBuf dataParser(data);
            if (dataParser.NextTok(' ') == "HTTP/1.1") {
                TStringBuf code = dataParser.NextTok(' ');
                if (code.size() == 3 && code[0] != '2') {
                    Error = dataParser.NextTok('\n');
                }
            }
        }
        PassAway();
    }
}

TString TViewerPipeClient::GetHTTPOK(TString contentType, TString response, TInstant lastModified) {
    return Viewer->GetHTTPOK(GetRequest(), std::move(contentType), std::move(response), lastModified);
}

TString TViewerPipeClient::GetHTTPOKJSON(TString response, TInstant lastModified) {
    return Viewer->GetHTTPOKJSON(GetRequest(), std::move(response), lastModified);
}

TString TViewerPipeClient::GetHTTPOKJSON(const NJson::TJsonValue& response, TInstant lastModified) {
    constexpr ui32 doubleNDigits = 17;
    constexpr ui32 floatNDigits = 9;
    constexpr EFloatToStringMode floatMode = EFloatToStringMode::PREC_NDIGITS;
    TStringStream content;
    NJson::WriteJson(&content, &response, {
        .DoubleNDigits = doubleNDigits,
        .FloatNDigits = floatNDigits,
        .FloatToStringMode = floatMode,
        .ValidateUtf8 = false,
        .WriteNanAsString = true,
    });
    return GetHTTPOKJSON(content.Str(), lastModified);
}

TString TViewerPipeClient::GetHTTPOKJSON(const google::protobuf::Message& response, TInstant lastModified) {
    TStringStream json;
    Proto2Json(response, json);
    return GetHTTPOKJSON(json.Str(), lastModified);
}

TString TViewerPipeClient::GetHTTPGATEWAYTIMEOUT(TString contentType, TString response) {
    return Viewer->GetHTTPGATEWAYTIMEOUT(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClient::GetHTTPBADREQUEST(TString contentType, TString response) {
    return Viewer->GetHTTPBADREQUEST(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClient::GetHTTPNOTFOUND(TString, TString) {
    return Viewer->GetHTTPNOTFOUND(GetRequest());
}

TString TViewerPipeClient::GetHTTPINTERNALERROR(TString contentType, TString response) {
    return Viewer->GetHTTPINTERNALERROR(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClient::GetHTTPFORBIDDEN(TString contentType, TString response) {
    return Viewer->GetHTTPFORBIDDEN(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClient::MakeForward(const std::vector<ui32>& nodes) {
    return Viewer->MakeForward(GetRequest(), nodes);
}

void TViewerPipeClient::RequestDone(i32 requests) {
    if (requests == 0) {
        return;
    }
    if (requests > DataRequests) {
        BLOG_ERROR("Requests count mismatch: " << requests << " > " << DataRequests);
        if (Span) {
            Span.Event("Requests count mismatch");
        }
        requests = DataRequests;
    }
    DataRequests -= requests;
    if (!DelayedRequests.empty()) {
        SendDelayedRequests();
    }
    if (DataRequests == 0 && !ReplySent) {
        ReplyAndPassAway();
    }
}

void TViewerPipeClient::CancelAllRequests() {
    DelayedRequests.clear();
}

void TViewerPipeClient::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        ui32 requests = FailPipeConnect(ev->Get()->TabletId);
        RequestDone(requests);
    }
}

void TViewerPipeClient::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    ui32 requests = FailPipeConnect(ev->Get()->TabletId);
    RequestDone(requests);
}

void TViewerPipeClient::HandleResolveResource(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    if (ResourceNavigateResponse) {
        ResourceNavigateResponse->Set(std::move(ev));
        if (ResourceNavigateResponse->IsOk()) {
            TSchemeCacheNavigate::TEntry& entry(ResourceNavigateResponse->Get()->Request->ResultSet.front());
            SharedDatabase = CanonizePath(entry.Path);
            Direct |= (SharedDatabase == AppData()->TenantName);
            ResourceBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(SharedDatabase);
            --DataRequests; // don't count this request
        } else {
            AddEvent("Failed to resolve database - shared database not found");
            Direct = true;
            Bootstrap(); // retry bootstrap without redirect this time
        }
    }
}

void TViewerPipeClient::HandleResolveDatabase(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    if (DatabaseNavigateResponse) {
        DatabaseNavigateResponse->Set(std::move(ev));
        if (DatabaseNavigateResponse->IsOk()) {
            TSchemeCacheNavigate::TEntry& entry(DatabaseNavigateResponse->Get()->Request->ResultSet.front());
            if (entry.DomainInfo && entry.DomainInfo->ResourcesDomainKey && entry.DomainInfo->DomainKey != entry.DomainInfo->ResourcesDomainKey) {
                ResourceNavigateResponse = MakeRequestSchemeCacheNavigate(TPathId(entry.DomainInfo->ResourcesDomainKey));
                --DataRequests; // don't count this request
                Become(&TViewerPipeClient::StateResolveResource);
            } else {
                Database = CanonizePath(entry.Path);
                DatabaseBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(Database);
                --DataRequests; // don't count this request
            }
        } else {
            AddEvent("Failed to resolve database - not found");
            Direct = true;
            Bootstrap(); // retry bootstrap without redirect this time
        }
    }
}

void TViewerPipeClient::HandleResolve(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
    if (DatabaseBoardInfoResponse) {
        DatabaseBoardInfoResponse->Set(std::move(ev));
        if (DatabaseBoardInfoResponse->IsOk()) {
            if (Direct) {
                return Bootstrap(); // retry bootstrap without redirect this time
            } else {
                return ReplyAndPassAway(MakeForward(GetNodesFromBoardReply(DatabaseBoardInfoResponse->GetRef())));
            }
        }
    }
    if (ResourceBoardInfoResponse) {
        ResourceBoardInfoResponse->Set(std::move(ev));
        if (ResourceBoardInfoResponse->IsOk()) {
            if (Direct) {
                return Bootstrap(); // retry bootstrap without redirect this time
            } else {
                return ReplyAndPassAway(MakeForward(GetNodesFromBoardReply(ResourceBoardInfoResponse->GetRef())));
            }
        }
    }
    AddEvent("Failed to resolve database nodes");
    Direct = true;
    Bootstrap(); // retry bootstrap without redirect this time
}

void TViewerPipeClient::HandleTimeout() {
    ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT());
}

STATEFN(TViewerPipeClient::StateResolveDatabase) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvStateStorage::TEvBoardInfo, HandleResolve);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveDatabase);
        cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
    }
}

STATEFN(TViewerPipeClient::StateResolveResource) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvStateStorage::TEvBoardInfo, HandleResolve);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveResource);
        cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
    }
}

void TViewerPipeClient::RedirectToDatabase(const TString& database) {
    DatabaseNavigateResponse = MakeRequestSchemeCacheNavigate(database);
    --DataRequests; // don't count this request
    Become(&TViewerPipeClient::StateResolveDatabase);
}

bool TViewerPipeClient::NeedToRedirect() {
    auto request = GetRequest();
    if (NeedRedirect && request) {
        NeedRedirect = false;
        Direct |= !request.GetHeader("X-Forwarded-From-Node").empty(); // we're already forwarding
        Direct |= (Database == AppData()->TenantName) || Database.empty(); // we're already on the right node or don't use database filter
        if (Database) {
            RedirectToDatabase(Database); // to find some dynamic node and redirect query there
            return true;
        }
        if (!Viewer->CheckAccessViewer(request)) {
            ReplyAndPassAway(GetHTTPFORBIDDEN("text/html", "<html><body><h1>403 Forbidden</h1></body></html>"), "Access denied");
            return true;
        }
    }
    return false;
}

void TViewerPipeClient::PassAway() {
    AddEvent("PassAway");
    if (Span) {
        if (Error) {
            Span.EndError(Error);
        } else {
            Span.EndOk();
        }
    }
    std::sort(SubscriptionNodeIds.begin(), SubscriptionNodeIds.end());
    SubscriptionNodeIds.erase(std::unique(SubscriptionNodeIds.begin(), SubscriptionNodeIds.end()), SubscriptionNodeIds.end());
    for (TNodeId nodeId : SubscriptionNodeIds) {
        Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
    }
    ClosePipes();
    CancelAllRequests();
    PassedAway = true;
    TBase::PassAway();
}

void TViewerPipeClient::AddEvent(const TString& name) {
    if (Span) {
        Span.Event(name);
    }
}

}

#include "json_pipe_req.h"
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NViewer {

NTabletPipe::TClientConfig TViewerPipeClient::GetPipeClientConfig() {
    NTabletPipe::TClientConfig clientConfig;
    if (WithRetry) {
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    }
    return clientConfig;
}

TViewerPipeClient::~TViewerPipeClient() = default;

TViewerPipeClient::TViewerPipeClient() = default;

TViewerPipeClient::TViewerPipeClient(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
    : Viewer(viewer)
    , Event(ev)
{
    InitConfig(Event->Get()->Request.GetParams());
    NWilson::TTraceId traceId;
    TStringBuf traceparent = Event->Get()->Request.GetHeader("traceparent");
    if (traceparent) {
        traceId = NWilson::TTraceId::FromTraceparentHeader(traceparent, TComponentTracingLevels::ProductionVerbose);
    }
    TStringBuf wantTrace = Event->Get()->Request.GetHeader("X-Want-Trace");
    TStringBuf traceVerbosity = Event->Get()->Request.GetHeader("X-Trace-Verbosity");
    TStringBuf traceTTL = Event->Get()->Request.GetHeader("X-Trace-TTL");
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
        Span = {TComponentTracingLevels::THttp::TopLevel, std::move(traceId), "http", NWilson::EFlags::AUTO_END};
        Span.Attribute("request_type", TString(Event->Get()->Request.GetUri().Before('?')));
    }
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
    if (DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
        TActivationContext::Send(event.release());
        ++Requests;
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
    while (!DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
        auto& request(DelayedRequests.front());
        TActivationContext::Send(request.Event.release());
        ++Requests;
        DelayedRequests.pop_front();
    }
}

TPathId TViewerPipeClient::GetPathId(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    if (ev->Get()->Request->ResultSet.size() == 1) {
        if (ev->Get()->Request->ResultSet.begin()->Self) {
            const auto& info = ev->Get()->Request->ResultSet.begin()->Self->Info;
            return TPathId(info.GetSchemeshardId(), info.GetPathId());
        }
        if (ev->Get()->Request->ResultSet.begin()->TableId) {
            return ev->Get()->Request->ResultSet.begin()->TableId.PathId;
        }
    }
    return {};
}

TString TViewerPipeClient::GetPath(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    if (ev->Get()->Request->ResultSet.size() == 1) {
        return CanonizePath(ev->Get()->Request->ResultSet.begin()->Path);
    }
    return {};
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

void TViewerPipeClient::RequestConsoleNodeConfigByTenant(TString tenant, ui64 cookie) {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    auto request = MakeHolder<NConsole::TEvConsole::TEvGetNodeConfigRequest>();
    request->Record.MutableNode()->SetTenant(tenant);
    request->Record.AddItemKinds(static_cast<ui32>(NKikimrConsole::TConfigItem::FeatureFlagsItem));
    SendRequestToPipe(pipeClient, request.Release(), cookie);
}

TViewerPipeClient::TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> TViewerPipeClient::MakeRequestConsoleListTenants() {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
    return MakeRequestToPipe<NConsole::TEvConsole::TEvListTenantsResponse>(pipeClient, request.Release());
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

void TViewerPipeClient::RequestBSControllerPDiskRestart(ui32 nodeId, ui32 pdiskId, bool force) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* restartPDisk = request->Record.MutableRequest()->AddCommand()->MutableRestartPDisk();
    restartPDisk->MutableTargetPDiskId()->SetNodeId(nodeId);
    restartPDisk->MutableTargetPDiskId()->SetPDiskId(pdiskId);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClient::RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
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
    SendRequestToPipe(pipeClient, request.Release());
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
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetGroupsResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> TViewerPipeClient::RequestBSControllerPools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetStoragePoolsResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClient::RequestBSControllerVSlots() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release());
}

TViewerPipeClient::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClient::RequestBSControllerPDisks() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release());
}

void TViewerPipeClient::RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* updateDriveStatus = request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
    updateDriveStatus->CopyFrom(driveStatus);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClient::RequestSchemeCacheNavigate(const TString& path) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void TViewerPipeClient::RequestSchemeCacheNavigate(const TPathId& pathId) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

TViewerPipeClient::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClient::MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
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
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0 /*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("path_id", pathId.ToString());
    }
    return response;
}

void TViewerPipeClient::RequestTxProxyDescribe(const TString& path) {
    THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
    request->Record.MutableDescribePath()->SetPath(path);
    SendRequest(MakeTxProxyID(), request.Release());
}

void TViewerPipeClient::RequestStateStorageEndpointsLookup(const TString& path) {
    RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(path),
                                                   SelfId(),
                                                   EBoardLookupMode::Second));
    ++Requests;
}

void TViewerPipeClient::RequestStateStorageMetadataCacheEndpointsLookup(const TString& path) {
    if (!AppData()->DomainsInfo->Domain) {
        return;
    }
    RegisterWithSameMailbox(CreateBoardLookupActor(MakeDatabaseMetadataCacheBoardPath(path),
                                                   SelfId(),
                                                   EBoardLookupMode::Second));
    ++Requests;
}

std::vector<TNodeId> TViewerPipeClient::GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
    std::vector<TNodeId> databaseNodes;
    if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
        for (const auto& [actorId, infoEntry] : ev->Get()->InfoEntries) {
            databaseNodes.emplace_back(actorId.NodeId());
        }
    }
    std::sort(databaseNodes.begin(), databaseNodes.end());
    databaseNodes.erase(std::unique(databaseNodes.begin(), databaseNodes.end()), databaseNodes.end());
    return databaseNodes;
}

void TViewerPipeClient::InitConfig(const TCgiParameters& params) {
    Followers = FromStringWithDefault(params.Get("followers"), Followers);
    Metrics = FromStringWithDefault(params.Get("metrics"), Metrics);
    WithRetry = FromStringWithDefault(params.Get("with_retry"), WithRetry);
    MaxRequestsInFlight = FromStringWithDefault(params.Get("max_requests_in_flight"), MaxRequestsInFlight);
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

ui32 TViewerPipeClient::FailPipeConnect(NNodeWhiteboard::TTabletId tabletId) {
    auto itPipeInfo = PipeInfo.find(tabletId);
    if (itPipeInfo != PipeInfo.end()) {
        ui32 requests = itPipeInfo->second.Requests;
        NTabletPipe::CloseClient(SelfId(), itPipeInfo->second.PipeClient);
        PipeInfo.erase(itPipeInfo);
        return requests;
    }
    return 0;
}

TRequestState TViewerPipeClient::GetRequest() const {
    return {Event->Get(), Span.GetTraceId()};
}

void TViewerPipeClient::ReplyAndPassAway(TString data, const TString& error) {
    Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    if (Span) {
        if (error) {
            Span.EndError(error);
        } else {
            Span.EndOk();
        }
    }
    PassAway();
}

TString TViewerPipeClient::GetHTTPOK(TString contentType, TString response, TInstant lastModified) {
    return Viewer->GetHTTPOK(GetRequest(), std::move(contentType), std::move(response), lastModified);
}

TString TViewerPipeClient::GetHTTPOKJSON(TString response, TInstant lastModified) {
    return Viewer->GetHTTPOKJSON(GetRequest(), std::move(response), lastModified);
}

TString TViewerPipeClient::GetHTTPOKJSON(const NJson::TJsonValue& response, TInstant lastModified) {
    return GetHTTPOKJSON(NJson::WriteJson(response, false), lastModified);
}

TString TViewerPipeClient::GetHTTPGATEWAYTIMEOUT(TString contentType, TString response) {
    return Viewer->GetHTTPGATEWAYTIMEOUT(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClient::GetHTTPBADREQUEST(TString contentType, TString response) {
    return Viewer->GetHTTPBADREQUEST(GetRequest(), std::move(contentType), std::move(response));
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

void TViewerPipeClient::RequestDone(ui32 requests) {
    Requests -= requests;
    if (!DelayedRequests.empty()) {
        SendDelayedRequests();
    }
    if (Requests == 0) {
        ReplyAndPassAway();
    }
}

void TViewerPipeClient::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        ui32 requests = FailPipeConnect(ev->Get()->TabletId);
        RequestDone(requests);
    }
}

void TViewerPipeClient::HandleTimeout() {
    ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT());
}

void TViewerPipeClient::PassAway() {
    ClosePipes();
    TBase::PassAway();
}

}

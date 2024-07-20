#include "json_pipe_req.h"

namespace NKikimr::NViewer {

// It's hack to get access to protected members
struct TViewerPipeClientImpl::TDerivedActor : IActor {
    TActorId CallRegisterWithSameMailbox(IActor* actor) {
        return RegisterWithSameMailbox(actor);
    }
    template<typename... Args>
    bool CallSend(Args&&... args) {
        return Send(std::forward<Args>(args)...);
    }
    void CallPassAway() {
        PassAway();
    }
};

NTabletPipe::TClientConfig TViewerPipeClientImpl::GetPipeClientConfig() {
    NTabletPipe::TClientConfig clientConfig;
    if (WithRetry) {
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    }
    return clientConfig;
}

TViewerPipeClientImpl::~TViewerPipeClientImpl() = default;

TViewerPipeClientImpl::TViewerPipeClientImpl(IActor* derived)
    : Derived(reinterpret_cast<TDerivedActor*>(derived))
{
}

TViewerPipeClientImpl::TViewerPipeClientImpl(IActor* derived, IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
    : Derived(reinterpret_cast<TDerivedActor*>(derived))
    , Viewer(viewer)
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

TActorId TViewerPipeClientImpl::ConnectTabletPipe(NNodeWhiteboard::TTabletId tabletId) {
    TPipeInfo& pipeInfo = PipeInfo[tabletId];
    if (!pipeInfo.PipeClient) {
        auto pipe = NTabletPipe::CreateClient(Derived->SelfId(), tabletId, GetPipeClientConfig());
        pipeInfo.PipeClient = Derived->CallRegisterWithSameMailbox(pipe);
    }
    pipeInfo.Requests++;
    return pipeInfo.PipeClient;
}

void TViewerPipeClientImpl::SendEvent(std::unique_ptr<IEventHandle> event) {
    if (DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
        TActivationContext::Send(event.release());
        ++Requests;
    } else {
        DelayedRequests.push_back({
            .Event = std::move(event),
        });
    }
}

void TViewerPipeClientImpl::SendRequest(TActorId recipient, IEventBase* ev, ui32 flags, ui64 cookie, NWilson::TTraceId traceId) {
    SendEvent(std::make_unique<IEventHandle>(recipient, Derived->SelfId(), ev, flags, cookie, nullptr/*forwardOnNondelivery*/, std::move(traceId)));
}

void TViewerPipeClientImpl::SendRequestToPipe(TActorId pipe, IEventBase* ev, ui64 cookie, NWilson::TTraceId traceId) {
    std::unique_ptr<IEventHandle> event = std::make_unique<IEventHandle>(pipe, Derived->SelfId(), ev, 0/*flags*/, cookie, nullptr/*forwardOnNondelivery*/, std::move(traceId));
    event->Rewrite(TEvTabletPipe::EvSend, pipe);
    SendEvent(std::move(event));
}

void TViewerPipeClientImpl::SendDelayedRequests() {
    while (!DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
        auto& request(DelayedRequests.front());
        TActivationContext::Send(request.Event.release());
        ++Requests;
        DelayedRequests.pop_front();
    }
}

void TViewerPipeClientImpl::RequestHiveDomainStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
    request->Record.SetReturnFollowers(Followers);
    request->Record.SetReturnMetrics(Metrics);
    SendRequestToPipe(pipeClient, request.Release(), hiveId);
}

void TViewerPipeClientImpl::RequestHiveNodeStats(NNodeWhiteboard::TTabletId hiveId, TPathId pathId) {
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

void TViewerPipeClientImpl::RequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
    SendRequestToPipe(pipeClient, request.Release(), hiveId);
}

TViewerPipeClientImpl::TRequestResponse<TEvHive::TEvResponseHiveStorageStats> TViewerPipeClientImpl::MakeRequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
    TActorId pipeClient = ConnectTabletPipe(hiveId);
    THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
    auto response = MakeRequestToPipe<TEvHive::TEvResponseHiveStorageStats>(pipeClient, request.Release(), hiveId);
    if (response.Span) {
        auto hive_id = "#" + ::ToString(hiveId);
        response.Span.Attribute("hive_id", hive_id);
    }
    return response;
}

void TViewerPipeClientImpl::RequestConsoleListTenants() {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClientImpl::TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> TViewerPipeClientImpl::MakeRequestConsoleListTenants() {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
    return MakeRequestToPipe<NConsole::TEvConsole::TEvListTenantsResponse>(pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestConsoleGetTenantStatus(const TString& path) {
    TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
    THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
    request->Record.MutableRequest()->set_path(path);
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerConfig() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerConfigWithStoragePools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClientImpl::TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> TViewerPipeClientImpl::MakeRequestBSControllerConfigWithStoragePools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
    return MakeRequestToPipe<TEvBlobStorage::TEvControllerConfigResponse>(pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerInfo() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvRequestControllerInfo> request = MakeHolder<TEvBlobStorage::TEvRequestControllerInfo>();
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    SendRequestToPipe(pipeClient, request.Release());
}

TViewerPipeClientImpl::TRequestResponse<TEvBlobStorage::TEvControllerSelectGroupsResult> TViewerPipeClientImpl::MakeRequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request, ui64 cookie) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    return MakeRequestToPipe<TEvBlobStorage::TEvControllerSelectGroupsResult>(pipeClient, request.Release(), cookie);
}

void TViewerPipeClientImpl::RequestBSControllerPDiskRestart(ui32 nodeId, ui32 pdiskId, bool force) {
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

void TViewerPipeClientImpl::RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force) {
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

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClientImpl::RequestBSControllerPDiskInfo(ui32 nodeId, ui32 pdiskId) {
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

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClientImpl::RequestBSControllerVDiskInfo(ui32 nodeId, ui32 pdiskId) {
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

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> TViewerPipeClientImpl::RequestBSControllerGroups() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetGroupsResponse>(pipeClient, request.release());
}

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> TViewerPipeClientImpl::RequestBSControllerPools() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetStoragePoolsResponse>(pipeClient, request.release());
}

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClientImpl::RequestBSControllerVSlots() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release());
}

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClientImpl::RequestBSControllerPDisks() {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release());
}

void TViewerPipeClientImpl::RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force) {
    TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* updateDriveStatus = request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
    updateDriveStatus->CopyFrom(driveStatus);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    SendRequestToPipe(pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestSchemeCacheNavigate(const TString& path) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void TViewerPipeClientImpl::RequestSchemeCacheNavigate(const TPathId& pathId) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

TViewerPipeClientImpl::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClientImpl::MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0/*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("cookie", "#" + ::ToString(cookie));
    }
    return response;
}

TViewerPipeClientImpl::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClientImpl::MakeRequestSchemeCacheNavigate(TPathId pathId, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0/*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("response", "#" + ::ToString(cookie));
    }
    return response;
}

void TViewerPipeClientImpl::RequestTxProxyDescribe(const TString& path) {
    THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
    request->Record.MutableDescribePath()->SetPath(path);
    SendRequest(MakeTxProxyID(), request.Release());
}

void TViewerPipeClientImpl::RequestStateStorageEndpointsLookup(const TString& path) {
    Derived->CallRegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(path),
                                                                Derived->SelfId(),
                                                                EBoardLookupMode::Second));
    ++Requests;
}

void TViewerPipeClientImpl::RequestStateStorageMetadataCacheEndpointsLookup(const TString& path) {
    if (!AppData()->DomainsInfo->Domain) {
        return;
    }
    Derived->CallRegisterWithSameMailbox(CreateBoardLookupActor(MakeDatabaseMetadataCacheBoardPath(path),
                                                                Derived->SelfId(),
                                                                EBoardLookupMode::Second));
    ++Requests;
}

std::vector<TNodeId> TViewerPipeClientImpl::GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
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

void TViewerPipeClientImpl::InitConfig(const TCgiParameters& params) {
    Followers = FromStringWithDefault(params.Get("followers"), Followers);
    Metrics = FromStringWithDefault(params.Get("metrics"), Metrics);
    WithRetry = FromStringWithDefault(params.Get("with_retry"), WithRetry);
}

void TViewerPipeClientImpl::InitConfig(const TRequestSettings& settings) {
    Followers = settings.Followers;
    Metrics = settings.Metrics;
    WithRetry = settings.WithRetry;
}

void TViewerPipeClientImpl::ClosePipes() {
    for (const auto& [tabletId, pipeInfo] : PipeInfo) {
        if (pipeInfo.PipeClient) {
            NTabletPipe::CloseClient(Derived->SelfId(), pipeInfo.PipeClient);
        }
    }
    PipeInfo.clear();
}

ui32 TViewerPipeClientImpl::FailPipeConnect(NNodeWhiteboard::TTabletId tabletId) {
    auto itPipeInfo = PipeInfo.find(tabletId);
    if (itPipeInfo != PipeInfo.end()) {
        ui32 requests = itPipeInfo->second.Requests;
        NTabletPipe::CloseClient(Derived->SelfId(), itPipeInfo->second.PipeClient);
        PipeInfo.erase(itPipeInfo);
        return requests;
    }
    return 0;
}

TRequestState TViewerPipeClientImpl::GetRequest() const {
    return {Event->Get(), Span.GetTraceId()};
}

void TViewerPipeClientImpl::ReplyAndPassAway(TString data, const TString& error) {
    Derived->CallSend(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    if (Span) {
        if (error) {
            Span.EndError(error);
        } else {
            Span.EndOk();
        }
    }
    Derived->CallPassAway();
}

TString TViewerPipeClientImpl::GetHTTPOK(TString contentType, TString response, TInstant lastModified) {
    return Viewer->GetHTTPOK(GetRequest(), std::move(contentType), std::move(response), lastModified);
}

TString TViewerPipeClientImpl::GetHTTPOKJSON(TString response, TInstant lastModified) {
    return Viewer->GetHTTPOKJSON(GetRequest(), std::move(response), lastModified);
}

TString TViewerPipeClientImpl::GetHTTPGATEWAYTIMEOUT(TString contentType, TString response) {
    return Viewer->GetHTTPGATEWAYTIMEOUT(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClientImpl::GetHTTPBADREQUEST(TString contentType, TString response) {
    return Viewer->GetHTTPBADREQUEST(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClientImpl::GetHTTPINTERNALERROR(TString contentType, TString response) {
    return Viewer->GetHTTPINTERNALERROR(GetRequest(), std::move(contentType), std::move(response));
}

TString TViewerPipeClientImpl::MakeForward(const std::vector<ui32>& nodes) {
    return Viewer->MakeForward(GetRequest(), nodes);
}

}

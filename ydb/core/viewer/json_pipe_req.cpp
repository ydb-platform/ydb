#include "json_pipe_req.h"

namespace NKikimr::NViewer {

NTabletPipe::TClientConfig TViewerPipeClientImpl::GetPipeClientConfig() {
    NTabletPipe::TClientConfig clientConfig;
    if (WithRetry) {
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    }
    return clientConfig;
}

TViewerPipeClientImpl::TViewerPipeClientImpl() = default;

TViewerPipeClientImpl::TViewerPipeClientImpl(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
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

void TViewerPipeClientImpl::SendRequest(TActorId selfId, TActorId recipient, IEventBase* ev, ui32 flags, ui64 cookie, NWilson::TTraceId traceId) {
    SendEvent(std::make_unique<IEventHandle>(recipient, selfId, ev, flags, cookie, nullptr/*forwardOnNondelivery*/, std::move(traceId)));
}

void TViewerPipeClientImpl::SendRequestToPipe(TActorId selfId, TActorId pipe, IEventBase* ev, ui64 cookie, NWilson::TTraceId traceId) {
    std::unique_ptr<IEventHandle> event = std::make_unique<IEventHandle>(pipe, selfId, ev, 0/*flags*/, cookie, nullptr/*forwardOnNondelivery*/, std::move(traceId));
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

void TViewerPipeClientImpl::RequestHiveDomainStats(TActorId selfId, TActorId pipeClient, NNodeWhiteboard::TTabletId hiveId) {
    THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
    request->Record.SetReturnFollowers(Followers);
    request->Record.SetReturnMetrics(Metrics);
    SendRequestToPipe(selfId, pipeClient, request.Release(), hiveId);
}

void TViewerPipeClientImpl::RequestHiveNodeStats(TActorId selfId, TActorId pipeClient, NNodeWhiteboard::TTabletId hiveId, TPathId pathId) {
    THolder<TEvHive::TEvRequestHiveNodeStats> request = MakeHolder<TEvHive::TEvRequestHiveNodeStats>();
    request->Record.SetReturnMetrics(Metrics);
    if (pathId != TPathId()) {
        request->Record.SetReturnExtendedTabletInfo(true);
        request->Record.SetFilterTabletsBySchemeShardId(pathId.OwnerId);
        request->Record.SetFilterTabletsByPathId(pathId.LocalPathId);
    }
    SendRequestToPipe(selfId, pipeClient, request.Release(), hiveId);
}

TViewerPipeClientImpl::TRequestResponse<TEvHive::TEvResponseHiveStorageStats> TViewerPipeClientImpl::MakeRequestHiveStorageStats(TActorId selfId, TActorId pipeClient, NNodeWhiteboard::TTabletId hiveId) {
    THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
    auto response = MakeRequestToPipe<TEvHive::TEvResponseHiveStorageStats>(selfId, pipeClient, request.Release(), hiveId);
    if (response.Span) {
        auto hive_id = "#" + ::ToString(hiveId);
        response.Span.Attribute("hive_id", hive_id);
    }
    return response;
}

void TViewerPipeClientImpl::RequestConsoleGetTenantStatus(TActorId selfId, TActorId pipeClient, const TString& path) {
    THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
    request->Record.MutableRequest()->set_path(path);
    SendRequestToPipe(selfId, pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerConfig(TActorId selfId, TActorId pipeClient) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    SendRequestToPipe(selfId, pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerConfigWithStoragePools(TActorId selfId, TActorId pipeClient) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
    SendRequestToPipe(selfId, pipeClient, request.Release());
}

TViewerPipeClientImpl::TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> TViewerPipeClientImpl::MakeRequestBSControllerConfigWithStoragePools(TActorId selfId, TActorId pipeClient) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
    return MakeRequestToPipe<TEvBlobStorage::TEvControllerConfigResponse>(selfId, pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerPDiskRestart(TActorId selfId, TActorId pipeClient, ui32 nodeId, ui32 pdiskId, bool force) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* restartPDisk = request->Record.MutableRequest()->AddCommand()->MutableRestartPDisk();
    restartPDisk->MutableTargetPDiskId()->SetNodeId(nodeId);
    restartPDisk->MutableTargetPDiskId()->SetPDiskId(pdiskId);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    SendRequestToPipe(selfId, pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestBSControllerVDiskEvict(TActorId selfId, TActorId pipeClient, ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force) {
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
    SendRequestToPipe(selfId, pipeClient, request.Release());
}

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> TViewerPipeClientImpl::RequestBSControllerPDiskInfo(TActorId selfId, TActorId pipeClient, ui32 nodeId, ui32 pdiskId) {
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
    request->Record.SetInclusiveFrom(true);
    request->Record.SetInclusiveTo(true);
    request->Record.MutableFrom()->SetNodeId(nodeId);
    request->Record.MutableFrom()->SetPDiskId(pdiskId);
    request->Record.MutableTo()->SetNodeId(nodeId);
    request->Record.MutableTo()->SetPDiskId(pdiskId);
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(selfId, pipeClient, request.release());
}

TViewerPipeClientImpl::TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> TViewerPipeClientImpl::RequestBSControllerVDiskInfo(TActorId selfId, TActorId pipeClient, ui32 nodeId, ui32 pdiskId) {
    auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
    request->Record.SetInclusiveFrom(true);
    request->Record.SetInclusiveTo(true);
    request->Record.MutableFrom()->SetNodeId(nodeId);
    request->Record.MutableFrom()->SetPDiskId(pdiskId);
    request->Record.MutableFrom()->SetVSlotId(0);
    request->Record.MutableTo()->SetNodeId(nodeId);
    request->Record.MutableTo()->SetPDiskId(pdiskId);
    request->Record.MutableTo()->SetVSlotId(std::numeric_limits<ui32>::max());
    return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(selfId, pipeClient, request.release());
}

void TViewerPipeClientImpl::RequestBSControllerPDiskUpdateStatus(TActorId selfId, TActorId pipeClient, const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force) {
    THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* updateDriveStatus = request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
    updateDriveStatus->CopyFrom(driveStatus);
    if (force) {
        request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
    }
    SendRequestToPipe(selfId, pipeClient, request.Release());
}

void TViewerPipeClientImpl::RequestSchemeCacheNavigate(TActorId selfId, const TString& path) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    SendRequest(selfId, MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void TViewerPipeClientImpl::RequestSchemeCacheNavigate(TActorId selfId, const TPathId& pathId) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    SendRequest(selfId, MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

TViewerPipeClientImpl::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClientImpl::MakeRequestSchemeCacheNavigate(TActorId selfId, const TString& path, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(selfId, MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0/*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("cookie", "#" + ::ToString(cookie));
    }
    return response;
}

TViewerPipeClientImpl::TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> TViewerPipeClientImpl::MakeRequestSchemeCacheNavigate(TActorId selfId, TPathId pathId, ui64 cookie) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    request->ResultSet.emplace_back(entry);
    auto response = MakeRequest<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(selfId, MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0/*flags*/, cookie);
    if (response.Span) {
        response.Span.Attribute("response", "#" + ::ToString(cookie));
    }
    return response;
}

void TViewerPipeClientImpl::RequestTxProxyDescribe(TActorId selfId, const TString& path) {
    THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
    request->Record.MutableDescribePath()->SetPath(path);
    SendRequest(selfId, MakeTxProxyID(), request.Release());
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

void TViewerPipeClientImpl::ClosePipes(const TActorIdentity& selfId) {
    for (const auto& [tabletId, pipeInfo] : PipeInfo) {
        if (pipeInfo.PipeClient) {
            NTabletPipe::CloseClient(selfId, pipeInfo.PipeClient);
        }
    }
    PipeInfo.clear();
}

ui32 TViewerPipeClientImpl::FailPipeConnect(const TActorIdentity& selfId, NNodeWhiteboard::TTabletId tabletId) {
    auto itPipeInfo = PipeInfo.find(tabletId);
    if (itPipeInfo != PipeInfo.end()) {
        ui32 requests = itPipeInfo->second.Requests;
        NTabletPipe::CloseClient(selfId, itPipeInfo->second.PipeClient);
        PipeInfo.erase(itPipeInfo);
        return requests;
    }
    return 0;
}

TRequestState TViewerPipeClientImpl::GetRequest() const {
    return {Event->Get(), Span.GetTraceId()};
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

#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/grpc_services/db_metadata_cache.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/sys_view/common/events.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NKikimr;
using namespace NSchemeCache;
using NNodeWhiteboard::TNodeId;

template <typename TDerived>
class TViewerPipeClient : public TActorBootstrapped<TDerived> {
protected:
    using TBase = TActorBootstrapped<TDerived>;
    bool Followers = true;
    bool Metrics = true;
    bool WithRetry = true;
    ui32 Requests = 0;
    static constexpr ui32 MaxRequestsInFlight = 50;
    NWilson::TSpan Span;
    IViewer* Viewer = nullptr;
    NMon::TEvHttpInfo::TPtr Event;

    struct TPipeInfo {
        TActorId PipeClient;
        ui32 Requests = 0;
    };

    std::unordered_map<NNodeWhiteboard::TTabletId, TPipeInfo> PipeInfo;

    struct TDelayedRequest {
        std::unique_ptr<IEventHandle> Event;
    };

    std::deque<TDelayedRequest> DelayedRequests;

    template<typename T>
    struct TRequestResponse {
        std::variant<std::monostate, std::unique_ptr<T>, TString> Response;
        NWilson::TSpan Span;

        TRequestResponse() = default;
        TRequestResponse(NWilson::TSpan&& span)
            : Span(std::move(span))
        {}

        TRequestResponse(const TRequestResponse&) = delete;
        TRequestResponse(TRequestResponse&&) = default;
        TRequestResponse& operator =(const TRequestResponse&) = delete;
        TRequestResponse& operator =(TRequestResponse&&) = default;

        void Set(std::unique_ptr<T>&& response) {
            if (!IsDone()) {
                Span.EndOk();
            }
            Response = std::move(response);
        }

        void Set(TAutoPtr<TEventHandle<T>>&& response) {
            Set(std::unique_ptr<T>(response->Release().Release()));
        }

        bool Error(const TString& error) {
            bool result = false;
            if (!IsDone()) {
                Span.EndError(error);
                result = true;
            }
            if (!IsOk()) {
                Response = error;
            }
            return result;
        }

        bool IsOk() const {
            return std::holds_alternative<std::unique_ptr<T>>(Response);
        }

        bool IsError() const {
            return std::holds_alternative<TString>(Response);
        }

        bool IsDone() const {
            return Response.index() != 0;
        }

        explicit operator bool() const {
            return IsOk();
        }

        T* Get() {
            return std::get<std::unique_ptr<T>>(Response).get();
        }

        const T* Get() const {
            return std::get<std::unique_ptr<T>>(Response).get();
        }

        T& GetRef() {
            return *Get();
        }

        const T& GetRef() const {
            return *Get();
        }

        T* operator ->() {
            return Get();
        }

        const T* operator ->() const {
            return Get();
        }

        T& operator *() {
            return GetRef();
        }

        const T& operator *() const {
            return GetRef();
        }

        TString GetError() const {
            return std::get<TString>(Response);
        }
    };

    NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        }
        return clientConfig;
    }

    TViewerPipeClient() = default;

    TViewerPipeClient(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
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

    TActorId ConnectTabletPipe(NNodeWhiteboard::TTabletId tabletId) {
        TPipeInfo& pipeInfo = PipeInfo[tabletId];
        if (!pipeInfo.PipeClient) {
            auto pipe = NTabletPipe::CreateClient(TBase::SelfId(), tabletId, GetPipeClientConfig());
            pipeInfo.PipeClient = TBase::RegisterWithSameMailbox(pipe);
        }
        pipeInfo.Requests++;
        return pipeInfo.PipeClient;
    }

    void SendEvent(std::unique_ptr<IEventHandle> event) {
        if (DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
            TActivationContext::Send(event.release());
            ++Requests;
        } else {
            DelayedRequests.push_back({
                .Event = std::move(event),
            });
        }
    }

    void SendRequest(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
        SendEvent(std::make_unique<IEventHandle>(recipient, TBase::SelfId(), ev, flags, cookie, nullptr/*forwardOnNondelivery*/, std::move(traceId)));
    }

    void SendRequestToPipe(const TActorId& pipe, IEventBase* ev, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
        std::unique_ptr<IEventHandle> event = std::make_unique<IEventHandle>(pipe, TBase::SelfId(), ev, 0/*flags*/, cookie, nullptr/*forwardOnNondelivery*/, std::move(traceId));
        event->Rewrite(TEvTabletPipe::EvSend, pipe);
        SendEvent(std::move(event));
    }

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequest(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequest(recipient, ev, flags, cookie, response.Span.GetTraceId());
        return response;
    }

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequestToPipe(const TActorId& pipe, IEventBase* ev, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequestToPipe(pipe, ev, cookie, response.Span.GetTraceId());
        return response;
    }

    void SendDelayedRequests() {
        while (!DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
            auto& request(DelayedRequests.front());
            TActivationContext::Send(request.Event.release());
            ++Requests;
            DelayedRequests.pop_front();
        }
    }

    void RequestHiveDomainStats(NNodeWhiteboard::TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
        request->Record.SetReturnFollowers(Followers);
        request->Record.SetReturnMetrics(Metrics);
        SendRequestToPipe(pipeClient, request.Release(), hiveId);
    }

    void RequestHiveNodeStats(NNodeWhiteboard::TTabletId hiveId, TPathId pathId) {
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

    void RequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
        SendRequestToPipe(pipeClient, request.Release(), hiveId);
    }

    TRequestResponse<TEvHive::TEvResponseHiveStorageStats> MakeRequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
        auto response = MakeRequestToPipe<TEvHive::TEvResponseHiveStorageStats>(pipeClient, request.Release(), hiveId);
        if (response.Span) {
            auto hive_id = "#" + ::ToString(hiveId);
            response.Span.Attribute("hive_id", hive_id);
        }
        return response;
    }

    NNodeWhiteboard::TTabletId GetConsoleId() {
        return MakeConsoleID();
    }

    void RequestConsoleListTenants() {
        TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
        THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
        SendRequestToPipe(pipeClient, request.Release());
    }

    TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> MakeRequestConsoleListTenants() {
        TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
        THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
        return MakeRequestToPipe<NConsole::TEvConsole::TEvListTenantsResponse>(pipeClient, request.Release());
    }

    void RequestConsoleGetTenantStatus(const TString& path) {
        TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
        THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
        request->Record.MutableRequest()->set_path(path);
        SendRequestToPipe(pipeClient, request.Release());
    }

    NNodeWhiteboard::TTabletId GetBSControllerId() {
        return MakeBSControllerID();
    }

    void RequestBSControllerConfig() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestBSControllerConfigWithStoragePools() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
        SendRequestToPipe(pipeClient, request.Release());
    }

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> MakeRequestBSControllerConfigWithStoragePools() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
        return MakeRequestToPipe<TEvBlobStorage::TEvControllerConfigResponse>(pipeClient, request.Release());
    }

    void RequestBSControllerInfo() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvRequestControllerInfo> request = MakeHolder<TEvBlobStorage::TEvRequestControllerInfo>();
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        SendRequestToPipe(pipeClient, request.Release());
    }

    TRequestResponse<TEvBlobStorage::TEvControllerSelectGroupsResult> MakeRequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request, ui64 cookie) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        return MakeRequestToPipe<TEvBlobStorage::TEvControllerSelectGroupsResult>(pipeClient, request.Release(), cookie);
    }

    void RequestBSControllerPDiskRestart(ui32 nodeId, ui32 pdiskId, bool force = false) {
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

    void RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force = false) {
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

    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDiskInfo(ui32 nodeId, ui32 pdiskId) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
        request->Record.SetInclusiveFrom(true);
        request->Record.SetInclusiveTo(true);
        request->Record.MutableFrom()->SetNodeId(nodeId);
        request->Record.MutableFrom()->SetPDiskId(pdiskId);
        request->Record.MutableTo()->SetNodeId(nodeId);
        request->Record.MutableTo()->SetPDiskId(pdiskId);
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release(), 0/*cookie*/);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVDiskInfo(ui32 nodeId, ui32 pdiskId) {
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
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release(), 0/*cookie*/);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> RequestBSControllerGroups() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetGroupsResponse>(pipeClient, request.release(), 0/*cookie*/);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> RequestBSControllerPools() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetStoragePoolsResponse>(pipeClient, request.release(), 0/*cookie*/);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVSlots() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release(), 0/*cookie*/);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDisks() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release(), 0/*cookie*/);
    }

    void RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force = false) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* updateDriveStatus = request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
        updateDriveStatus->CopyFrom(driveStatus);
        if (force) {
            request->Record.MutableRequest()->SetIgnoreDegradedGroupsChecks(true);
        }
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestSchemeCacheNavigate(const TString& path) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(path);
        entry.RedirectRequired = false;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
        request->ResultSet.emplace_back(entry);
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void RequestSchemeCacheNavigate(const TPathId& pathId) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId.PathId = pathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
        request->ResultSet.emplace_back(entry);
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie = 0) {
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

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TPathId& pathId, ui64 cookie = 0) {
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

    void RequestTxProxyDescribe(const TString& path) {
        THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
        request->Record.MutableDescribePath()->SetPath(path);
        SendRequest(MakeTxProxyID(), request.Release());
    }

    void RequestStateStorageEndpointsLookup(const TString& path) {
        TBase::RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(path),
                                                              TBase::SelfId(),
                                                              EBoardLookupMode::Second));
        ++Requests;
    }

    void RequestStateStorageMetadataCacheEndpointsLookup(const TString& path) {
        if (!AppData()->DomainsInfo->Domain) {
            return;
        }
        TBase::RegisterWithSameMailbox(CreateBoardLookupActor(MakeDatabaseMetadataCacheBoardPath(path),
                                                              TBase::SelfId(),
                                                              EBoardLookupMode::Second));
        ++Requests;
    }

    std::vector<TNodeId> GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
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

    void InitConfig(const TCgiParameters& params) {
        Followers = FromStringWithDefault(params.Get("followers"), Followers);
        Metrics = FromStringWithDefault(params.Get("metrics"), Metrics);
        WithRetry = FromStringWithDefault(params.Get("with_retry"), WithRetry);
    }

    void InitConfig(const TRequestSettings& settings) {
        Followers = settings.Followers;
        Metrics = settings.Metrics;
        WithRetry = settings.WithRetry;
    }

    void ClosePipes() {
        for (const auto& [tabletId, pipeInfo] : PipeInfo) {
            if (pipeInfo.PipeClient) {
                NTabletPipe::CloseClient(TBase::SelfId(), pipeInfo.PipeClient);
            }
        }
        PipeInfo.clear();
    }

    ui32 FailPipeConnect(NNodeWhiteboard::TTabletId tabletId) {
        auto itPipeInfo = PipeInfo.find(tabletId);
        if (itPipeInfo != PipeInfo.end()) {
            ui32 requests = itPipeInfo->second.Requests;
            NTabletPipe::CloseClient(TBase::SelfId(), itPipeInfo->second.PipeClient);
            PipeInfo.erase(itPipeInfo);
            return requests;
        }
        return 0;
    }

    void RequestDone(ui32 requests = 1) {
        Requests -= requests;
        if (!DelayedRequests.empty()) {
            SendDelayedRequests();
        }
        if (Requests == 0) {
            static_cast<TDerived*>(this)->ReplyAndPassAway();
        }
    }

    bool IsLastRequest() const {
        return Requests == 1;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ui32 requests = FailPipeConnect(ev->Get()->TabletId);
            RequestDone(requests);
        }
    }

    void PassAway() override {
        ClosePipes();
        TBase::PassAway();
    }

    TRequestState GetRequest() const {
        return {Event->Get(), Span.GetTraceId()};
    }

    void ReplyAndPassAway(TString data, const TString& error = {}) {
        TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        if (Span) {
            if (error) {
                Span.EndError(error);
            } else {
                Span.EndOk();
            }
        }
        PassAway();
    }

    TString GetHTTPOK(TString contentType = {}, TString response = {}, TInstant lastModified = {}) {
        return Viewer->GetHTTPOK(GetRequest(), contentType, response, lastModified);
    }

    TString GetHTTPOKJSON(TString response = {}, TInstant lastModified = {}) {
        return Viewer->GetHTTPOKJSON(GetRequest(), response, lastModified);
    }

    TString GetHTTPGATEWAYTIMEOUT(TString contentType = {}, TString response = {}) {
        return Viewer->GetHTTPGATEWAYTIMEOUT(GetRequest(), contentType, response);
    }

    TString GetHTTPBADREQUEST(TString contentType = {}, TString response = {}) {
        return Viewer->GetHTTPBADREQUEST(GetRequest(), contentType, response);
    }

    TString GetHTTPINTERNALERROR(TString contentType = {}, TString response = {}) {
        return Viewer->GetHTTPINTERNALERROR(GetRequest(), contentType, response);
    }

    TString MakeForward(const std::vector<ui32>& nodes) {
        return Viewer->MakeForward(GetRequest(), nodes);
    }
};

}
}

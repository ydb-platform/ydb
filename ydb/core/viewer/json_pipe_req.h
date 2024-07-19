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

class TViewerPipeClientImpl {
protected:
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

    NTabletPipe::TClientConfig GetPipeClientConfig();

    ~TViewerPipeClientImpl();

    TViewerPipeClientImpl();

    TViewerPipeClientImpl(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev);

    void SendEvent(std::unique_ptr<IEventHandle> event);

    void SendRequest(TActorId selfId, TActorId recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {});

    void SendRequestToPipe(TActorId selfId, TActorId pipe, IEventBase* ev, ui64 cookie = 0, NWilson::TTraceId traceId = {});

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequest(TActorId selfId, TActorId recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequest(selfId, recipient, ev, flags, cookie, response.Span.GetTraceId());
        return response;
    }

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequestToPipe(TActorId selfId, TActorId pipe, IEventBase* ev, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequestToPipe(selfId, pipe, ev, cookie, response.Span.GetTraceId());
        return response;
    }

    void SendDelayedRequests();

    void RequestHiveDomainStats(TActorId selfId, TActorId pipeClient, NNodeWhiteboard::TTabletId hiveId);

    void RequestHiveNodeStats(TActorId selfId, TActorId pipeClient, NNodeWhiteboard::TTabletId hiveId, TPathId pathId);

    TRequestResponse<TEvHive::TEvResponseHiveStorageStats> MakeRequestHiveStorageStats(TActorId selfId, TActorId pipeClient, NNodeWhiteboard::TTabletId hiveId);

    NNodeWhiteboard::TTabletId GetConsoleId() {
        return MakeConsoleID();
    }

    void RequestConsoleGetTenantStatus(TActorId selfId, TActorId pipeClient, const TString& path);

    NNodeWhiteboard::TTabletId GetBSControllerId() {
        return MakeBSControllerID();
    }

    void RequestBSControllerConfig(TActorId selfId, TActorId pipeClient);

    void RequestBSControllerConfigWithStoragePools(TActorId selfId, TActorId pipeClient);

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> MakeRequestBSControllerConfigWithStoragePools(TActorId selfId, TActorId pipeClient);

    void RequestBSControllerPDiskRestart(TActorId selfId, TActorId pipeClient, ui32 nodeId, ui32 pdiskId, bool force);

    void RequestBSControllerVDiskEvict(TActorId selfId, TActorId pipeClient, ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force);

    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDiskInfo(TActorId selfId, TActorId pipeClient, ui32 nodeId, ui32 pdiskId);

    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVDiskInfo(TActorId selfId, TActorId pipeClient, ui32 nodeId, ui32 pdiskId);

    void RequestBSControllerPDiskUpdateStatus(TActorId selfId, TActorId pipeClient, const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force);

    void RequestSchemeCacheNavigate(TActorId selfId, const TString& path);

    void RequestSchemeCacheNavigate(TActorId selfId, const TPathId& pathId);

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(TActorId selfId, const TString& path, ui64 cookie);

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(TActorId selfId, TPathId pathId, ui64 cookie);

    void RequestTxProxyDescribe(TActorId selfId, const TString& path);

    std::vector<TNodeId> GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev);

    void InitConfig(const TCgiParameters& params);

    void InitConfig(const TRequestSettings& settings);

    void ClosePipes(const TActorIdentity& selfId);

    ui32 FailPipeConnect(const TActorIdentity& selfId, NNodeWhiteboard::TTabletId tabletId);

    bool IsLastRequest() const {
        return Requests == 1;
    }

    TRequestState GetRequest() const;

    TString GetHTTPOK(TString contentType = {}, TString response = {}, TInstant lastModified = {});

    TString GetHTTPOKJSON(TString response = {}, TInstant lastModified = {});

    TString GetHTTPGATEWAYTIMEOUT(TString contentType = {}, TString response = {});

    TString GetHTTPBADREQUEST(TString contentType = {}, TString response = {});

    TString GetHTTPINTERNALERROR(TString contentType = {}, TString response = {});

    TString MakeForward(const std::vector<ui32>& nodes);
};

template <typename TDerived>
class TViewerPipeClient : public TActorBootstrapped<TDerived>, protected TViewerPipeClientImpl {
protected:
    using TBase = TActorBootstrapped<TDerived>;
    using TImpl = TViewerPipeClientImpl;

    using TImpl::TImpl;

    TActorId ConnectTabletPipe(NNodeWhiteboard::TTabletId tabletId) {
        TPipeInfo& pipeInfo = PipeInfo[tabletId];
        if (!pipeInfo.PipeClient) {
            auto pipe = NTabletPipe::CreateClient(TBase::SelfId(), tabletId, GetPipeClientConfig());
            pipeInfo.PipeClient = TBase::RegisterWithSameMailbox(pipe);
        }
        pipeInfo.Requests++;
        return pipeInfo.PipeClient;
    }

    void SendRequest(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
        TImpl::SendRequest(TBase::SelfId(), recipient, ev, flags, cookie, std::move(traceId));
    }

    void SendRequestToPipe(const TActorId& pipe, IEventBase* ev, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
        TImpl::SendRequestToPipe(TBase::SelfId(), pipe, ev, cookie, std::move(traceId));
    }

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequest(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        return TImpl::MakeRequest<TResponse>(TBase::SelfId(), recipient, ev, flags, cookie);
    }

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequestToPipe(const TActorId& pipe, IEventBase* ev, ui64 cookie = 0) {
        return TImpl::MakeRequestToPipe<TResponse>(TBase::SelfId(), pipe, ev, cookie);
    }

    void RequestHiveDomainStats(NNodeWhiteboard::TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        TImpl::RequestHiveDomainStats(TBase::SelfId(), pipeClient, hiveId);
    }

    void RequestHiveNodeStats(NNodeWhiteboard::TTabletId hiveId, TPathId pathId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        TImpl::RequestHiveNodeStats(TBase::SelfId(), pipeClient, hiveId, pathId);
    }

    void RequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
        SendRequestToPipe(pipeClient, request.Release(), hiveId);
    }

    TRequestResponse<TEvHive::TEvResponseHiveStorageStats> MakeRequestHiveStorageStats(NNodeWhiteboard::TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        return TImpl::MakeRequestHiveStorageStats(TBase::SelfId(), pipeClient, hiveId);
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
        TImpl::RequestConsoleGetTenantStatus(TBase::SelfId(), pipeClient, path);
    }

    void RequestBSControllerConfig() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        TImpl::RequestBSControllerConfig(TBase::SelfId(), pipeClient);
    }

    void RequestBSControllerConfigWithStoragePools() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        TImpl::RequestBSControllerConfigWithStoragePools(TBase::SelfId(), pipeClient);
    }

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> MakeRequestBSControllerConfigWithStoragePools() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        return TImpl::MakeRequestBSControllerConfigWithStoragePools(TBase::SelfId(), pipeClient);
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
        TImpl::RequestBSControllerPDiskRestart(TBase::SelfId(), pipeClient, nodeId, pdiskId, force);
    }

    void RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force = false) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        TImpl::RequestBSControllerVDiskEvict(TBase::SelfId(), pipeClient, groupId, groupGeneration, failRealmIdx, failDomainIdx, vdiskIdx, force);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDiskInfo(ui32 nodeId, ui32 pdiskId) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        return TImpl::RequestBSControllerPDiskInfo(TBase::SelfId(), pipeClient, nodeId, pdiskId);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVDiskInfo(ui32 nodeId, ui32 pdiskId) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        return TImpl::RequestBSControllerVDiskInfo(TBase::SelfId(), pipeClient, nodeId, pdiskId);
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> RequestBSControllerGroups() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetGroupsResponse>(pipeClient, request.release());
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> RequestBSControllerPools() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetStoragePoolsResponse>(pipeClient, request.release());
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVSlots() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetVSlotsResponse>(pipeClient, request.release());
    }

    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDisks() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        auto request = std::make_unique<NSysView::TEvSysView::TEvGetPDisksRequest>();
        return MakeRequestToPipe<NSysView::TEvSysView::TEvGetPDisksResponse>(pipeClient, request.release());
    }

    void RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force = false) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        TImpl::RequestBSControllerPDiskUpdateStatus(TBase::SelfId(), pipeClient, driveStatus, force);
    }

    void RequestSchemeCacheNavigate(const TString& path) {
        TImpl::RequestSchemeCacheNavigate(TBase::SelfId(), path);
    }

    void RequestSchemeCacheNavigate(const TPathId& pathId) {
        TImpl::RequestSchemeCacheNavigate(TBase::SelfId(), pathId);
    }

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie = 0) {
        return TImpl::MakeRequestSchemeCacheNavigate(TBase::SelfId(), path, cookie);
    }

    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TPathId& pathId, ui64 cookie = 0) {
        return TImpl::MakeRequestSchemeCacheNavigate(TBase::SelfId(), pathId, cookie);
    }

    void RequestTxProxyDescribe(const TString& path) {
        TImpl::RequestTxProxyDescribe(TBase::SelfId(), path);
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

    void RequestDone(ui32 requests = 1) {
        Requests -= requests;
        if (!DelayedRequests.empty()) {
            SendDelayedRequests();
        }
        if (Requests == 0) {
            static_cast<TDerived*>(this)->ReplyAndPassAway();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ui32 requests = FailPipeConnect(TBase::SelfId(), ev->Get()->TabletId);
            RequestDone(requests);
        }
    }

    void PassAway() override {
        ClosePipes(TBase::SelfId());
        TBase::PassAway();
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
};

}
}

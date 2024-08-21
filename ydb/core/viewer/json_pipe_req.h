#pragma once
#include "viewer.h"
#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/db_metadata_cache.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NViewer {

using namespace NKikimr;
using namespace NSchemeCache;
using NNodeWhiteboard::TNodeId;
using NNodeWhiteboard::TTabletId;

class TViewerPipeClient : public TActorBootstrapped<TViewerPipeClient> {
    using TBase = TActorBootstrapped<TViewerPipeClient>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    virtual void Bootstrap() = 0;
    virtual void ReplyAndPassAway() = 0;

protected:
    bool Followers = true;
    bool Metrics = true;
    bool WithRetry = true;
    ui32 Requests = 0;
    ui32 MaxRequestsInFlight = 50;
    NWilson::TSpan Span;
    IViewer* Viewer = nullptr;
    NMon::TEvHttpInfo::TPtr Event;

    struct TPipeInfo {
        TActorId PipeClient;
        ui32 Requests = 0;
    };

    std::unordered_map<TTabletId, TPipeInfo> PipeInfo;

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

    ~TViewerPipeClient();
    TViewerPipeClient();
    TViewerPipeClient(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev);
    TActorId ConnectTabletPipe(TTabletId tabletId);
    void SendEvent(std::unique_ptr<IEventHandle> event);
    void SendRequest(TActorId recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {});
    void SendRequestToPipe(TActorId pipe, IEventBase* ev, ui64 cookie = 0, NWilson::TTraceId traceId = {});

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequest(TActorId recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequest(recipient, ev, flags, cookie, response.Span.GetTraceId());
        return response;
    }

    template<typename TResponse>
    TRequestResponse<TResponse> MakeRequestToPipe(TActorId pipe, IEventBase* ev, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequestToPipe(pipe, ev, cookie, response.Span.GetTraceId());
        return response;
    }

    void SendDelayedRequests();
    void RequestHiveDomainStats(TTabletId hiveId);
    void RequestHiveNodeStats(TTabletId hiveId, TPathId pathId);
    void RequestHiveStorageStats(TTabletId hiveId);

    TTabletId GetConsoleId() {
        return MakeConsoleID();
    }

    TTabletId GetBSControllerId() {
        return MakeBSControllerID();
    }

    static TPathId GetPathId(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    static TString GetPath(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    TRequestResponse<TEvHive::TEvResponseHiveDomainStats> MakeRequestHiveDomainStats(TTabletId hiveId);
    TRequestResponse<TEvHive::TEvResponseHiveStorageStats> MakeRequestHiveStorageStats(TTabletId hiveId);
    void RequestConsoleListTenants();
    TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> MakeRequestConsoleListTenants();
    void RequestConsoleGetTenantStatus(const TString& path);
    TRequestResponse<NConsole::TEvConsole::TEvGetTenantStatusResponse> MakeRequestConsoleGetTenantStatus(const TString& path);
    void RequestBSControllerConfig();
    void RequestBSControllerConfigWithStoragePools();
    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> MakeRequestBSControllerConfigWithStoragePools();
    void RequestBSControllerInfo();
    void RequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request);
    TRequestResponse<TEvBlobStorage::TEvControllerSelectGroupsResult> MakeRequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request, ui64 cookie = 0);
    void RequestBSControllerPDiskRestart(ui32 nodeId, ui32 pdiskId, bool force = false);
    void RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force = false);
    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDiskInfo(ui32 nodeId, ui32 pdiskId);
    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVDiskInfo(ui32 nodeId, ui32 pdiskId);
    TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> RequestBSControllerGroups();
    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> RequestBSControllerPools();
    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVSlots();
    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDisks();
    void RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force = false);
    void RequestSchemeCacheNavigate(const TString& path);
    void RequestSchemeCacheNavigate(const TPathId& pathId);
    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie = 0);
    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(TPathId pathId, ui64 cookie = 0);
    TRequestResponse<TEvViewer::TEvViewerResponse> MakeRequestViewer(TNodeId nodeId, TEvViewer::TEvViewerRequest* request, ui32 flags = 0);
    void RequestTxProxyDescribe(const TString& path);
    void RequestStateStorageEndpointsLookup(const TString& path);
    void RequestStateStorageMetadataCacheEndpointsLookup(const TString& path);
    std::vector<TNodeId> GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev);
    void InitConfig(const TCgiParameters& params);
    void InitConfig(const TRequestSettings& settings);
    void ClosePipes();
    ui32 FailPipeConnect(TTabletId tabletId);

    bool IsLastRequest() const {
        return Requests == 1;
    }

    bool NoMoreRequests(ui32 requestsDone = 0) const {
        return Requests == requestsDone;
    }

    TRequestState GetRequest() const;
    void ReplyAndPassAway(TString data, const TString& error = {});

    TString GetHTTPOK(TString contentType = {}, TString response = {}, TInstant lastModified = {});
    TString GetHTTPOKJSON(TString response = {}, TInstant lastModified = {});
    TString GetHTTPOKJSON(const NJson::TJsonValue& response, TInstant lastModified = {});
    TString GetHTTPGATEWAYTIMEOUT(TString contentType = {}, TString response = {});
    TString GetHTTPBADREQUEST(TString contentType = {}, TString response = {});
    TString GetHTTPINTERNALERROR(TString contentType = {}, TString response = {});
    TString GetHTTPFORBIDDEN(TString contentType = {}, TString response = {});
    TString MakeForward(const std::vector<ui32>& nodes);

    void RequestDone(ui32 requests = 1);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void HandleTimeout();
    void PassAway() override;
};

}

#pragma once
#include "viewer.h"
#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/db_metadata_cache.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NViewer {

using namespace NKikimr;
using namespace NSchemeCache;
using namespace NProtobufJson;
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
    bool WithRetry = false;
    TString Database;
    TString SharedDatabase;
    bool Direct = false;
    bool NeedRedirect = true;
    i32 DataRequests = 0; // how many requests we wait to process data
    bool PassedAway = false;
    bool ReplySent = false;
    bool UseCache = false;
    TDuration CachedDataMaxAge;
    TString Error;
    i32 MaxRequestsInFlight = 200;
    NWilson::TSpan Span;
    IViewer* Viewer = nullptr;
    NMon::TEvHttpInfo::TPtr Event;
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr HttpEvent;
    TCgiParameters Params;
    NJson::TJsonValue PostData;
    TJsonSettings JsonSettings;
    TProto2JsonConfig Proto2JsonConfig;
    TDuration Timeout = TDuration::Seconds(10);

    struct TPipeInfo {
        TActorId PipeClient;
        i32 Requests = 0;
    };

    std::unordered_map<TTabletId, TPipeInfo> PipeInfo;

    struct TDelayedRequest {
        std::unique_ptr<IEventHandle> Event;
    };

    std::deque<TDelayedRequest> DelayedRequests;
    std::vector<TNodeId> SubscriptionNodeIds;

    template<typename T>
    struct TRequestResponse {
        std::variant<std::monostate, std::shared_ptr<T>, TString> Response;
        NWilson::TSpan Span;

        TRequestResponse() = default;
        TRequestResponse(NWilson::TSpan&& span)
            : Span(std::move(span))
        {}

        TRequestResponse(const TRequestResponse&) = delete;
        TRequestResponse& operator =(const TRequestResponse& other) = delete;
        TRequestResponse(TRequestResponse&&) = default;
        TRequestResponse& operator =(TRequestResponse&&) = default;

        TRequestResponse(std::shared_ptr<T>&& response)
            : Response(std::move(response))
        {}

        void SetInternal(std::shared_ptr<T>&& response) {
            Response = std::move(response);
        }

        bool Set(std::shared_ptr<T>&& response) {
            constexpr bool hasErrorCheck = requires(const T& r) {TViewerPipeClient::IsSuccess(r);};
            constexpr bool hasUpdateCache = requires(std::shared_ptr<T>&& r) {TEvViewer::TEvUpdateSharedCacheTabletResponse(r);};
            if constexpr (hasErrorCheck) {
                if (!TViewerPipeClient::IsSuccess(*response)) {
                    return Error(TViewerPipeClient::GetError(*response));
                }
            }
            if (Span) {
                Span.EndOk();
            }
            if constexpr (hasUpdateCache) {
                TActivationContext::Send(MakeViewerID(TActivationContext::ActorSystem()->NodeId), std::make_unique<TEvViewer::TEvUpdateSharedCacheTabletResponse>(response));
            }
            if (IsDone()) {
                return false;
            }
            Response = std::move(response);
            return true;
        }

        bool Set(TAutoPtr<TEventHandle<T>>&& response) {
            return Set(std::shared_ptr<T>(response->Release().Release()));
        }

        bool Error(const TString& error) {
            if (!IsDone()) {
                Span.EndError(error);
                Response = error;
                return true;
            }
            return false;
        }

        bool IsOk() const {
            return std::holds_alternative<std::shared_ptr<T>>(Response);
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
            return std::get<std::shared_ptr<T>>(Response).get();
        }

        const T* Get() const {
            return std::get<std::shared_ptr<T>>(Response).get();
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

        void Event(const TString& name) {
            if (Span) {
                Span.Event(name);
            }
        }
    };

    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> DatabaseNavigateResponse;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> ResourceNavigateResponse;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> DatabaseBoardInfoResponse;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> ResourceBoardInfoResponse;

    NTabletPipe::TClientConfig GetPipeClientConfig();

    ~TViewerPipeClient();
    TViewerPipeClient();
    TViewerPipeClient(NWilson::TTraceId traceId);
    TViewerPipeClient(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev, const TString& handlerName = {});
    TViewerPipeClient(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, const TString& handlerName = {});
    TActorId ConnectTabletPipe(TTabletId tabletId);
    void SendEvent(std::unique_ptr<IEventHandle> event);
    void SendRequest(TActorId recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {});
    void SendRequestToPipe(TActorId pipe, IEventBase* ev, ui64 cookie = 0, NWilson::TTraceId traceId = {});

    template<typename TResponse>
    [[nodiscard]] TRequestResponse<TResponse> MakeRequest(TActorId recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequest(recipient, ev, flags, cookie, response.Span.GetTraceId());
        if (flags & IEventHandle::FlagSubscribeOnSession) {
            SubscriptionNodeIds.push_back(recipient.NodeId());
        }
        return response;
    }

    template<typename TResponse>
    [[nodiscard]] TRequestResponse<TResponse> MakeRequestToPipe(TActorId pipe, IEventBase* ev, ui64 cookie = 0) {
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        SendRequestToPipe(pipe, ev, cookie, response.Span.GetTraceId());
        return response;
    }

    template<typename TResponse>
    [[nodiscard]] TRequestResponse<TResponse> MakeRequestToTablet(TTabletId tabletId, IEventBase* ev, ui64 cookie = 0) {
        TActorId pipe = ConnectTabletPipe(tabletId);
        TRequestResponse<TResponse> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        if (response.Span) {
            response.Span.Attribute("tablet_id", "#" + ::ToString(tabletId));
        }
        SendRequestToPipe(pipe, ev, cookie, response.Span.GetTraceId());
        return response;
    }

    template<typename TRequest>
    [[nodiscard]] TRequestResponse<typename NNodeWhiteboard::WhiteboardResponse<TRequest>::Type> MakeWhiteboardRequest(TNodeId nodeId, TRequest* ev, ui32 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession) {
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
        TRequestResponse<typename NNodeWhiteboard::WhiteboardResponse<TRequest>::Type> response(Span.CreateChild(TComponentTracingLevels::THttp::Detailed, TypeName(*ev)));
        if (response.Span) {
            response.Span.Attribute("target_node_id", nodeId);
        }
        SendRequest(whiteboardServiceId, ev, flags, nodeId, response.Span.GetTraceId());
        return response;
    }

    [[nodiscard]] TRequestResponse<TEvViewer::TEvViewerResponse> MakeViewerRequest(TNodeId nodeId, TEvViewer::TEvViewerRequest* ev, ui32 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
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

    static TPathId GetPathId(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev);
    static TString GetPath(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev);

    static TPathId GetPathId(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    static TString GetPath(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    static bool IsSuccess(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev);
    static TString GetError(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& ev);

    static bool IsSuccess(const TEvStateStorage::TEvBoardInfo& ev);
    static TString GetError(const TEvStateStorage::TEvBoardInfo& ev);

    static bool IsSuccess(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult& ev);
    static TString GetError(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult& ev);

    static bool IsSuccess(const TEvTxUserProxy::TEvProposeTransactionStatus& ev);
    static TString GetError(const TEvTxUserProxy::TEvProposeTransactionStatus& ev);

    static bool IsSuccess(const NKqp::TEvGetScriptExecutionOperationResponse& ev);
    static TString GetError(const NKqp::TEvGetScriptExecutionOperationResponse& ev);

    void UpdateSharedCacheTablet(TTabletId tabletId, std::unique_ptr<IEventBase> request);

    [[nodiscard]] TRequestResponse<TEvHive::TEvResponseHiveDomainStats> MakeRequestHiveDomainStats(TTabletId hiveId);
    [[nodiscard]] TRequestResponse<TEvHive::TEvResponseHiveStorageStats> MakeRequestHiveStorageStats(TTabletId hiveId);
    [[nodiscard]] TRequestResponse<TEvHive::TEvResponseHiveNodeStats> MakeRequestHiveNodeStats(TTabletId hiveId, TEvHive::TEvRequestHiveNodeStats* request);
    void RequestConsoleListTenants();
    [[nodiscard]] TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse> MakeRequestConsoleListTenants();
    [[nodiscard]] TRequestResponse<NConsole::TEvConsole::TEvGetNodeConfigResponse> MakeRequestConsoleNodeConfigByTenant(TString tenant, ui64 cookie = 0);
    [[nodiscard]] TRequestResponse<NConsole::TEvConsole::TEvGetAllConfigsResponse> MakeRequestConsoleGetAllConfigs();
    void RequestConsoleGetTenantStatus(const TString& path);
    [[nodiscard]] TRequestResponse<NConsole::TEvConsole::TEvGetTenantStatusResponse> MakeRequestConsoleGetTenantStatus(const TString& path);
    void RequestBSControllerConfig();
    void RequestBSControllerConfigWithStoragePools();
    [[nodiscard]] TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> MakeRequestBSControllerConfigWithStoragePools();
    void RequestBSControllerInfo();
    void RequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request);
    [[nodiscard]] TRequestResponse<TEvBlobStorage::TEvControllerSelectGroupsResult> MakeRequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request, ui64 cookie = 0);
    [[nodiscard]] TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> RequestBSControllerPDiskRestart(ui32 nodeId, ui32 pdiskId, bool force = false);
    [[nodiscard]] TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> RequestBSControllerVDiskEvict(ui32 groupId, ui32 groupGeneration, ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool force = false);
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDiskInfo(ui32 nodeId, ui32 pdiskId);
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVDiskInfo(ui32 nodeId, ui32 pdiskId);
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> RequestBSControllerGroups();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> RequestBSControllerPools();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> RequestBSControllerVSlots();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> RequestBSControllerPDisks();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetStorageStatsResponse> RequestBSControllerStorageStats();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse> MakeCachedRequestBSControllerGroups();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> MakeCachedRequestBSControllerPools();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> MakeCachedRequestBSControllerVSlots();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> MakeCachedRequestBSControllerPDisks();
    [[nodiscard]] TRequestResponse<NSysView::TEvSysView::TEvGetStorageStatsResponse> MakeCachedRequestBSControllerStorageStats();
    [[nodiscard]] TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> RequestBSControllerPDiskUpdateStatus(const NKikimrBlobStorage::TUpdateDriveStatus& driveStatus, bool force = false);

    THolder<NSchemeCache::TSchemeCacheNavigate> SchemeCacheNavigateRequestBuilder(NSchemeCache::TSchemeCacheNavigate::TEntry&& entry);

    void RequestSchemeCacheNavigate(const TString& path);
    void RequestSchemeCacheNavigate(const TPathId& pathId);

    [[nodiscard]] TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie = 0);
    [[nodiscard]] TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(TPathId pathId, ui64 cookie = 0);
    [[nodiscard]] TRequestResponse<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> MakeRequestSchemeShardDescribe(TTabletId schemeShardId, const TString& path, const NKikimrSchemeOp::TDescribeOptions& options = {}, ui64 cookie = 0);
    [[nodiscard]] TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigateWithToken(
        const TString& path, ui32 access, ui64 cookie = 0);

    TRequestResponse<TEvViewer::TEvViewerResponse> MakeRequestViewer(TNodeId nodeId, TEvViewer::TEvViewerRequest* request, ui32 flags = 0);
    void RequestTxProxyDescribe(const TString& path, const NKikimrSchemeOp::TDescribeOptions& options = {});
    void RequestStateStorageEndpointsLookup(const TString& path);
    TRequestResponse<TEvStateStorage::TEvBoardInfo> MakeRequestStateStorageMetadataCacheEndpointsLookup(const TString& path, ui64 cookie = 0);
    TRequestResponse<TEvStateStorage::TEvBoardInfo> MakeRequestStateStorageEndpointsLookup(const TString& path, ui64 cookie = 0);
    std::vector<TNodeId> GetNodesFromBoardReply(TEvStateStorage::TEvBoardInfo::TPtr& ev);
    std::vector<TNodeId> GetNodesFromBoardReply(const TEvStateStorage::TEvBoardInfo& ev);
    std::vector<TNodeId> GetDatabaseNodes();
    bool IsDatabaseRequest() const;
    void InitConfig(const TCgiParameters& params);
    void InitConfig(const TRequestSettings& settings);
    void BuildParamsFromJson(TStringBuf data);
    void BuildParamsFromFormData(TStringBuf data);
    void SetupTracing(const TString& handlerName);
    void ApplyForceMode(TEvBlobStorage::TEvControllerConfigRequest& request);

    template<typename TJson>
    void Proto2Json(const NProtoBuf::Message& proto, TJson& json) {
        try {
            NProtobufJson::Proto2Json(proto, json, Proto2JsonConfig);
        }
        catch (const std::exception& e) {
            json = TStringBuilder() << "error converting " << proto.GetTypeName() << " to json: " << e.what();
        }
    }

    void ClosePipes();
    i32 FailPipeConnect(TTabletId tabletId);

    bool IsLastRequest() const {
        return DataRequests == 1;
    }

    bool WaitingForResponse() const {
        return DataRequests != 0;
    }

    bool NoMoreRequests(i32 requestsDone = 0) const {
        return DataRequests == requestsDone;
    }

    TRequestState GetRequest() const;
    void ReplyAndPassAway(TString data, const TString& error = {});

    TString GetHTTPOK(TString contentType = {}, TString response = {}, TInstant lastModified = {});
    TString GetHTTPOKJSON(TString response = {}, TInstant lastModified = {});
    TString GetHTTPOKJSON(const NJson::TJsonValue& response, TInstant lastModified = {});
    TString GetHTTPOKJSON(const google::protobuf::Message& response, TInstant lastModified = {});
    TString GetHTTPGATEWAYTIMEOUT(TString contentType = {}, TString response = {});
    TString GetHTTPBADREQUEST(TString contentType = {}, TString response = {});
    TString GetHTTPNOTFOUND(TString contentType = {}, TString response = {});
    TString GetHTTPINTERNALERROR(TString contentType = {}, TString response = {});
    TString GETHTTPACCESSDENIED(TString contentType = {}, TString response = {});
    TString MakeForward(const std::vector<ui32>& nodes);

    void RequestDone(i32 requests = 1);
    void CacheRequestDone();
    void CancelAllRequests();
    void AddEvent(const TString& name);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);
    void HandleResolveDatabase(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleResolveResource(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleResolve(TEvStateStorage::TEvBoardInfo::TPtr& ev);
    STATEFN(StateResolveDatabase);
    STATEFN(StateResolveResource);
    void RedirectToDatabase(const TString& database);
    bool NeedToRedirect(bool checkDatabaseAuth = true);
    void HandleTimeout();
    void PassAway() override;
};

}

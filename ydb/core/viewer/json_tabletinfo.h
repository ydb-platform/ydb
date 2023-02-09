#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_pipe.h>
#include "json_pipe_req.h"
#include "json_wb_req.h"
#include <span>

namespace NKikimr {
namespace NViewer {

template<>
struct TWhiteboardInfo<TEvWhiteboard::TEvTabletStateResponse> {
    using TResponseType = TEvWhiteboard::TEvTabletStateResponse;
    using TElementType = NKikimrWhiteboard::TTabletStateInfo;
    using TElementTypePacked5 = NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponsePacked5;
    using TElementKeyType = std::pair<ui64, ui32>;

    static constexpr bool StaticNodesOnly = false;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType* response) {
        return *response->Record.MutableTabletStateInfo();
    }

    static std::span<const TElementTypePacked5> GetElementsFieldPacked5(TResponseType* response) {
        const auto& packed5 = response->Record.GetPacked5();
        return std::span{reinterpret_cast<const TElementTypePacked5*>(packed5.data()), packed5.size() / sizeof(TElementTypePacked5)};
    }

    static size_t GetElementsCount(TResponseType* response) {
        return response->Record.GetTabletStateInfo().size() + response->Record.GetPacked5().size() / sizeof(TElementTypePacked5);
    }

    static TElementKeyType GetElementKey(const TElementType& type) {
        return TElementKeyType(type.GetTabletId(), type.GetFollowerId());
    }

    static TElementKeyType GetElementKey(const TElementTypePacked5& type) {
        return TElementKeyType(type.TabletId, type.FollowerId);
    }

    static TString GetDefaultMergeField() {
        return "TabletId,FollowerId";
    }

    static THolder<TResponseType> MergeResponses(TMap<ui32, THolder<TResponseType>>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            TStaticMergeKey<TResponseType> mergeKey;
            return TWhiteboardMerger<TResponseType>::MergeResponsesBaseHybrid(responses, mergeKey);
        } else {
            TWhiteboardMerger<TResponseType>::TDynamicMergeKey mergeKey(fields);
            return TWhiteboardMerger<TResponseType>::MergeResponsesBase(responses, mergeKey);
        }
    }
};

template <>
struct TWhiteboardMergerComparator<NKikimrWhiteboard::TTabletStateInfo> {
    bool operator ()(const NKikimrWhiteboard::TTabletStateInfo& a, const NKikimrWhiteboard::TTabletStateInfo& b) const {
        return std::make_tuple(a.GetGeneration(), a.GetChangeTime()) < std::make_tuple(b.GetGeneration(), b.GetChangeTime());
    }
};

template <>
struct TWhiteboardMergerComparator<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponsePacked5> {
    bool operator ()(const NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponsePacked5& a, const NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponsePacked5& b) const {
        return a.Generation < b.Generation;
    }
};

class TJsonTabletInfo : public TJsonWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse> {
    static const bool WithRetry = false;
    using TBase = TJsonWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse>;
    using TThis = TJsonTabletInfo;
    TVector<ui64> Tablets;
public:
    TJsonTabletInfo(IViewer *viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TJsonWhiteboardRequest(viewer, ev)
    {
        static TString prefix = "json/tabletinfo ";
        LogPrefix = prefix;
    }

    static NTabletPipe::TClientConfig InitPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        }
        return clientConfig;
    }

    static const NTabletPipe::TClientConfig& GetPipeClientConfig() {
        static NTabletPipe::TClientConfig clientConfig = InitPipeClientConfig();
        return clientConfig;
    }

    void Bootstrap() override {
        BLOG_TRACE("Bootstrap()");
        const auto& params(Event->Get()->Request.GetParams());
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        if (params.Has("path")) {
            THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
            if (!Event->Get()->UserToken.empty()) {
                request->Record.SetUserToken(Event->Get()->UserToken);
            }
            NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
            record->SetPath(params.Get("path"));

            TActorId txproxy = MakeTxProxyID();
            TBase::Send(txproxy, request.Release());
            Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
        } else {
            TBase::Bootstrap();
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev) {
        THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> describeResult = ev->Release();
        if (describeResult->GetRecord().GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
            const auto& pathDescription = describeResult->GetRecord().GetPathDescription();
            if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeTable) {
                Tablets.reserve(describeResult->GetRecord().GetPathDescription().TablePartitionsSize());
                for (const auto& partition : describeResult->GetRecord().GetPathDescription().GetTablePartitions()) {
                    Tablets.emplace_back(partition.GetDatashardId());
                }
            }
            if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup) {
                Tablets.reserve(describeResult->GetRecord().GetPathDescription().GetPersQueueGroup().PartitionsSize());
                for (const auto& partition : describeResult->GetRecord().GetPathDescription().GetPersQueueGroup().GetPartitions()) {
                    Tablets.emplace_back(partition.GetTabletId());
                }
            }
            if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeDir
                || pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeSubDomain
                || pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain) {
                if (pathDescription.HasDomainDescription()) {
                    for (TTabletId tabletId : pathDescription.GetDomainDescription().GetProcessingParams().GetCoordinators()) {
                        Tablets.emplace_back(tabletId);
                    }
                    for (TTabletId tabletId : pathDescription.GetDomainDescription().GetProcessingParams().GetMediators()) {
                        Tablets.emplace_back(tabletId);
                    }
                    if (pathDescription.GetDomainDescription().GetProcessingParams().HasSchemeShard()) {
                        Tablets.emplace_back(pathDescription.GetDomainDescription().GetProcessingParams().GetSchemeShard());
                    } else {
                        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
                        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;

                        Tablets.emplace_back(domain->SchemeRoot);

                        ui32 hiveDomain = domains->GetHiveDomainUid(domain->DefaultHiveUid);
                        ui64 defaultStateStorageGroup = domains->GetDefaultStateStorageGroup(hiveDomain);
                        Tablets.emplace_back(MakeBSControllerID(defaultStateStorageGroup));
                        Tablets.emplace_back(MakeConsoleID(defaultStateStorageGroup));
                        Tablets.emplace_back(MakeNodeBrokerID(defaultStateStorageGroup));
                    }
                    if (pathDescription.GetDomainDescription().GetProcessingParams().HasHive()) {
                        Tablets.emplace_back(pathDescription.GetDomainDescription().GetProcessingParams().GetHive());
                    }
                }
            }
            Sort(Tablets);
            Tablets.erase(std::unique(Tablets.begin(), Tablets.end()), Tablets.end());
        }
        if (Tablets.empty()) {
            ReplyAndPassAway();
        }
        TBase::Bootstrap();
    }

    virtual void FilterResponse(THolder<TEvWhiteboard::TEvTabletStateResponse>& response) override {
        if (!Tablets.empty()) {
            if (response != nullptr) {
                TAutoPtr<TEvWhiteboard::TEvTabletStateResponse> result = new TEvWhiteboard::TEvTabletStateResponse();
                for (const NKikimrWhiteboard::TTabletStateInfo& info : response->Record.GetTabletStateInfo()) {
                    if (BinarySearch(Tablets.begin(), Tablets.end(), info.GetTabletId())) {
                        result->Record.MutableTabletStateInfo()->Add()->CopyFrom(info);
                    }
                }
                result->Record.SetResponseTime(response->Record.GetResponseTime());
                response = result;
            }
        }
        if (response != nullptr) {
            for (NKikimrWhiteboard::TTabletStateInfo& info : *response->Record.MutableTabletStateInfo()) {
                info.SetOverall(GetWhiteboardFlag(GetFlagFromTabletState(info.GetState())));
            }
        }
        TBase::FilterResponse(response);
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void PassAway() override {
        TBase::PassAway();
    }
};

template <>
struct TJsonRequestParameters<TJsonTabletInfo> {
    static TString GetParameters() {
        return R"___([{"name":"node_id","in":"query","description":"node identifier","required":false,"type":"integer"},)___"
               R"___({"name":"path","in":"query","description":"schema path","required":false,"type":"string"},)___"
               R"___({"name":"merge","in":"query","description":"merge information from nodes","required":false,"type":"boolean"},)___"
               R"___({"name":"group","in":"query","description":"group information by field","required":false,"type":"string"},)___"
               R"___({"name":"all","in":"query","description":"return all possible key combinations (for enums only)","required":false,"type":"boolean"},)___"
               R"___({"name":"filter","in":"query","description":"filter information by field","required":false,"type":"string"},)___"
               R"___({"name":"alive","in":"query","description":"request from alive (connected) nodes only","required":false,"type":"boolean"},)___"
               R"___({"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},)___"
               R"___({"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},)___"
               R"___({"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"},)___"
               R"___({"name":"retries","in":"query","description":"number of retries","required":false,"type":"integer"},)___"
               R"___({"name":"retry_period","in":"query","description":"retry period in ms","required":false,"type":"integer","default":500},)___"
               R"___({"name":"static","in":"query","description":"request from static nodes only","required":false,"type":"boolean"},)___"
               R"___({"name":"since","in":"query","description":"filter by update time","required":false,"type":"string"}])___";
    }
};

template <>
struct TJsonRequestSchema<TJsonTabletInfo> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrWhiteboard::TEvTabletStateResponse>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestSummary<TJsonTabletInfo> {
    static TString GetSummary() {
        return "\"Информация о таблетках\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonTabletInfo> {
    static TString GetDescription() {
        return "\"Возвращает информацию о статусе таблеток в кластере\"";
    }
};

}
}

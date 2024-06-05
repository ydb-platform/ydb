#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/util/wildcard.h>
#include "json_pipe_req.h"
#include "json_wb_req.h"
#include <span>

namespace NKikimr {
namespace NViewer {

template<>
struct TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse> {
    using TResponseEventType = TEvWhiteboard::TEvTabletStateResponse;
    using TResponseType = NKikimrWhiteboard::TEvTabletStateResponse;
    using TElementType = NKikimrWhiteboard::TTabletStateInfo;
    using TElementTypePacked5 = NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponsePacked5;
    using TElementKeyType = std::pair<ui64, ui32>;

    static constexpr bool StaticNodesOnly = false;

    static ::google::protobuf::RepeatedPtrField<TElementType>& GetElementsField(TResponseType& response) {
        return *response.MutableTabletStateInfo();
    }

    static std::span<const TElementTypePacked5> GetElementsFieldPacked5(const TResponseType& response) {
        const auto& packed5 = response.GetPacked5();
        return std::span{reinterpret_cast<const TElementTypePacked5*>(packed5.data()), packed5.size() / sizeof(TElementTypePacked5)};
    }

    static size_t GetElementsCount(const TResponseType& response) {
        return response.GetTabletStateInfo().size() + response.GetPacked5().size() / sizeof(TElementTypePacked5);
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

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields = GetDefaultMergeField()) {
        if (fields == GetDefaultMergeField()) {
            TStaticMergeKey<TResponseType> mergeKey;
            TWhiteboardMerger<TResponseType>::MergeResponsesBaseHybrid(result, responses, mergeKey);
        } else {
            TWhiteboardMerger<TResponseType>::TDynamicMergeKey mergeKey(fields);
            TWhiteboardMerger<TResponseType>::MergeResponsesBase(result, responses, mergeKey);
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
    bool ReplyWithDeadTabletsInfo;
    using TBase = TJsonWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse>;
    using TThis = TJsonTabletInfo;
    THashMap<ui64, NKikimrTabletBase::TTabletTypes::EType> Tablets;
    TTabletId HiveId;
public:
    TJsonTabletInfo(IViewer *viewer, NMon::TEvHttpInfo::TPtr &ev)
        : TJsonWhiteboardRequest(viewer, ev)
    {
        static TString prefix = "json/tabletinfo ";
        LogPrefix = prefix;
    }

    void Bootstrap() override {
        BLOG_TRACE("Bootstrap()");
        const auto& params(Event->Get()->Request.GetParams());
        ReplyWithDeadTabletsInfo = params.Has("path");
        if (params.Has("path")) {
            TBase::RequestSettings.Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
            THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
            if (!Event->Get()->UserToken.empty()) {
                request->Record.SetUserToken(Event->Get()->UserToken);
            }
            NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
            record->SetPath(params.Get("path"));

            TActorId txproxy = MakeTxProxyID();
            TBase::Send(txproxy, request.Release());
            UnsafeBecome(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(TBase::RequestSettings.Timeout), new TEvents::TEvWakeup());
        } else {
            TBase::Bootstrap();
            if (!TBase::RequestSettings.FilterFields.empty()) {
                if (IsMatchesWildcard(TBase::RequestSettings.FilterFields, "(TabletId=*)")) {
                    TString strTabletId(TBase::RequestSettings.FilterFields.substr(10, TBase::RequestSettings.FilterFields.size() - 11));
                    TTabletId uiTabletId(FromStringWithDefault<TTabletId>(strTabletId, {}));
                    if (uiTabletId) {
                        Tablets[uiTabletId] = NKikimrTabletBase::TTabletTypes::Unknown;
                        Request->Record.AddFilterTabletId(uiTabletId);
                    }
                }
            }
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev) {
        const NKikimrScheme::TEvDescribeSchemeResult &rec = ev->Get()->GetRecord();
        HiveId = rec.GetPathDescription().GetDomainDescription().GetProcessingParams().GetHive();

        THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> describeResult = ev->Release();
        if (describeResult->GetRecord().GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
            const auto& pathDescription = describeResult->GetRecord().GetPathDescription();
            if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeTable) {
                for (const auto& partition : describeResult->GetRecord().GetPathDescription().GetTablePartitions()) {
                    Tablets[partition.GetDatashardId()] = NKikimrTabletBase::TTabletTypes::DataShard;
                }
            }
            if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup) {
                Tablets.reserve(describeResult->GetRecord().GetPathDescription().GetPersQueueGroup().PartitionsSize());
                for (const auto& partition : describeResult->GetRecord().GetPathDescription().GetPersQueueGroup().GetPartitions()) {
                    Tablets[partition.GetTabletId()] = NKikimrTabletBase::TTabletTypes::PersQueue;
                }
            }
            if (pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeDir
                || pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeSubDomain
                || pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain) {
                if (pathDescription.HasDomainDescription()) {
                    for (TTabletId tabletId : pathDescription.GetDomainDescription().GetProcessingParams().GetCoordinators()) {
                        Tablets[tabletId] = NKikimrTabletBase::TTabletTypes::Coordinator;
                    }
                    for (TTabletId tabletId : pathDescription.GetDomainDescription().GetProcessingParams().GetMediators()) {
                        Tablets[tabletId] = NKikimrTabletBase::TTabletTypes::Mediator;
                    }
                    if (pathDescription.GetDomainDescription().GetProcessingParams().HasSchemeShard()) {
                        Tablets[pathDescription.GetDomainDescription().GetProcessingParams().GetSchemeShard()] = NKikimrTabletBase::TTabletTypes::SchemeShard;
                    } else {
                        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
                        auto *domain = domains->GetDomain();
                        Tablets[domain->SchemeRoot] = NKikimrTabletBase::TTabletTypes::SchemeShard;
                        Tablets[MakeBSControllerID()] = NKikimrTabletBase::TTabletTypes::BSController;
                        Tablets[MakeConsoleID()] = NKikimrTabletBase::TTabletTypes::Console;
                        Tablets[MakeNodeBrokerID()] = NKikimrTabletBase::TTabletTypes::NodeBroker;
                    }
                    if (pathDescription.GetDomainDescription().GetProcessingParams().HasHive()) {
                        Tablets[pathDescription.GetDomainDescription().GetProcessingParams().GetHive()] = NKikimrTabletBase::TTabletTypes::Hive;
                    }
                }
            }
        }
        if (Tablets.empty()) {
            ReplyAndPassAway();
        } else {
            TBase::Bootstrap();
            for (auto tablet : Tablets) {
                Request->Record.AddFilterTabletId(tablet.first);
            }
        }
    }

    virtual void FilterResponse(NKikimrWhiteboard::TEvTabletStateResponse& response) override {
        if (!Tablets.empty()) {
            NKikimrWhiteboard::TEvTabletStateResponse result;
            for (const NKikimrWhiteboard::TTabletStateInfo& info : response.GetTabletStateInfo()) {
                auto tablet = Tablets.find(info.GetTabletId());
                if (tablet != Tablets.end()) {
                    result.MutableTabletStateInfo()->Add()->CopyFrom(info);
                    Tablets.erase(tablet->first);
                }
            }
            if (ReplyWithDeadTabletsInfo) {
                for (auto tablet : Tablets) {
                    auto deadTablet = result.MutableTabletStateInfo()->Add();
                    deadTablet->SetTabletId(tablet.first);
                    deadTablet->SetState(NKikimrWhiteboard::TTabletStateInfo::Dead);
                    deadTablet->SetType(tablet.second);
                    deadTablet->SetHiveId(HiveId);
                }
            }
            result.SetResponseTime(response.GetResponseTime());
            response = std::move(result);
        }
        for (NKikimrWhiteboard::TTabletStateInfo& info : *response.MutableTabletStateInfo()) {
            info.SetOverall(GetWhiteboardFlag(GetFlagFromTabletState(info.GetState())));
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
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: node_id
              in: query
              description: node identifier
              required: false
              type: integer
            - name: path
              in: query
              description: schema path
              required: false
              type: string
            - name: merge
              in: query
              description: merge information from nodes
              required: false
              type: boolean
            - name: group
              in: query
              description: group information by field
              required: false
              type: string
            - name: all
              in: query
              description: return all possible key combinations (for enums only)
              required: false
              type: boolean
            - name: filter
              in: query
              description: filter information by field
              required: false
              type: string
            - name: alive
              in: query
              description: request from alive (connected) nodes only
              required: false
              type: boolean
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            - name: retries
              in: query
              description: number of retries
              required: false
              type: integer
            - name: retry_period
              in: query
              description: retry period in ms
              required: false
              type: integer
              default: 500
            - name: static
              in: query
              description: request from static nodes only
              required: false
              type: boolean
            - name: since
              in: query
              description: filter by update time
              required: false
              type: string
            )___");
    }
};

template <>
struct TJsonRequestSchema<TJsonTabletInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrWhiteboard::TEvTabletStateResponse>();
    }
};

template <>
struct TJsonRequestSummary<TJsonTabletInfo> {
    static TString GetSummary() {
        return "Tablet information";
    }
};

template <>
struct TJsonRequestDescription<TJsonTabletInfo> {
    static TString GetDescription() {
        return "Returns information about tablets";
    }
};

}
}

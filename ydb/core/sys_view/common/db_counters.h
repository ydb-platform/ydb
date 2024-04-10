#pragma once

#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/protos/labeled_counters.pb.h>

#include <util/generic/hash.h>

namespace NKikimr {
namespace NSysView {

class TDbServiceCounters {
    NKikimrSysView::TDbServiceCounters ProtoCounters;

    THashMap<NKikimrTabletBase::TTabletTypes::EType, NKikimrSysView::TDbTabletCounters*> ByTabletType;

    using TGRpcRequestDesc = std::pair<TString, TString>;
    THashMap<TGRpcRequestDesc, NKikimrSysView::TDbGRpcCounters*> ByGRpcRequest;
    THashMap<TString, NKikimrSysView::TDbLabeledCounters*> ByGroupName;

public:
    void Clear()
    {
        ProtoCounters.Clear();
        ByTabletType.clear();
        ByGRpcRequest.clear();
        ByGroupName.clear();
    }

    void Swap(TDbServiceCounters& other) {
        ProtoCounters.Swap(&other.ProtoCounters);
        ByTabletType.swap(other.ByTabletType);
        ByGRpcRequest.swap(other.ByGRpcRequest);
        ByGroupName.swap(other.ByGroupName);
    }

    NKikimrSysView::TDbServiceCounters& Proto() { return ProtoCounters; }
    const NKikimrSysView::TDbServiceCounters& Proto() const { return ProtoCounters; }

    NKikimrSysView::TDbTabletCounters* FindTabletCounters(
        NKikimrTabletBase::TTabletTypes::EType tabletType) const
    {
        if (auto it = ByTabletType.find(tabletType); it != ByTabletType.end()) {
            return it->second;
        }
        return {};
    }

    NKikimrSysView::TDbTabletCounters* FindOrAddTabletCounters(
        NKikimrTabletBase::TTabletTypes::EType tabletType)
    {
        if (auto it = ByTabletType.find(tabletType); it != ByTabletType.end()) {
            return it->second;
        }

        auto* counters = ProtoCounters.AddTabletCounters();
        counters->SetType(tabletType);
        ByTabletType[tabletType] = counters;

        return counters;
    }

    NKikimrSysView::TDbGRpcCounters* FindGRpcCounters(
        const TString& grpcService, const TString& grpcRequest) const
    {
        auto key = std::make_pair(grpcService, grpcRequest);
        if (auto it = ByGRpcRequest.find(key); it != ByGRpcRequest.end()) {
            return it->second;
        }
        return {};
    }

    NKikimrSysView::TDbGRpcCounters* FindOrAddGRpcCounters(
        const TString& grpcService, const TString& grpcRequest)
    {
        auto key = std::make_pair(grpcService, grpcRequest);
        if (auto it = ByGRpcRequest.find(key); it != ByGRpcRequest.end()) {
            return it->second;
        }

        auto* counters = ProtoCounters.AddGRpcCounters();
        counters->SetGRpcService(grpcService);
        counters->SetGRpcRequest(grpcRequest);
        ByGRpcRequest[key] = counters;

        return counters;
    }

    NKikimrSysView::TDbLabeledCounters* FindLabeledCounters(const TString& groupName) const
    {
        if (auto it = ByGroupName.find(groupName); it != ByGroupName.end()) {
            return it->second;
        }
        return {};
    }

    NKikimrSysView::TDbLabeledCounters* FindOrAddLabeledCounters(const TString& groupName)
    {
        if (auto it = ByGroupName.find(groupName); it != ByGroupName.end()) {
            return it->second;
        }

        auto* counters = ProtoCounters.AddLabeledCounters();
        auto lCounters = counters->MutableAggregatedPerTablets();
        lCounters->SetGroup(groupName);
        lCounters->SetDelimiter("|");
        ByGroupName[groupName] = counters;

        return counters;
    }
};

} // NSysView
} // NKikimr

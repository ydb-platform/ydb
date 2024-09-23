#pragma once
#include "json_storage_base.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

using ::google::protobuf::FieldDescriptor;

class TJsonStorageUsage : public TJsonStorageBase {
    using TBase = TJsonStorageBase;
    using TThis = TJsonStorageUsage;
    ui32 Pace = 5;

public:
    TJsonStorageUsage(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        Pace = FromStringWithDefault<ui32>(params.Get("pace"), Pace);
        if (Pace == 0) {
            Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            PassAway();
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TBase::Handle(ev, false);
    }

    void ReplyAndPassAway() override {
        if (CheckAdditionalNodesInfoNeeded()) {
            return;
        }
        CollectDiskInfo(true);
        ParseVDisksFromBaseConfig();

        TMap<ui32, ui32> buckets;
        for (const auto& [poolName, poolInfo] : StoragePoolInfo) {
            if ((!FilterTenant.empty() || !FilterStoragePools.empty()) && FilterStoragePools.count(poolName) == 0) {
                continue;
            }
            for (TString groupId : poolInfo.Groups) {
                auto ib = BSGroupIndex.find(groupId);
                if (ib != BSGroupIndex.end()) {
                    int64 used = 0;
                    int64 limit = 0;
                    const auto& vDiskIds = ib->second.GetVDiskIds();
                    for (auto iv = vDiskIds.begin(); iv != vDiskIds.end(); ++iv) {
                        const NKikimrBlobStorage::TVDiskID& vDiskId = *iv;
                        auto ie = VDisksIndex.find(vDiskId);
                        if (ie != VDisksIndex.end()) {
                            used += ie->second.GetAllocatedSize();
                            limit += ie->second.GetAllocatedSize() + ie->second.GetAvailableSize();
                        }
                    }
                    int bucketNumber = limit == 0 ? 100 : used * 100 / limit / Pace;
                    if (!buckets.contains(bucketNumber)) {
                        buckets[bucketNumber] = 0;
                    }
                    buckets[bucketNumber]++;
                }
            }
        }
        NKikimrViewer::TStorageUsageStats StorageStats;
        StorageStats.SetPace(Pace);
        for (ui32 i = 0; i * Pace < 100; i++) {
            StorageStats.AddBuckets(buckets[i]);
        }

        TStringStream json;
        TProtoToJson::ProtoToJson(json, StorageStats, JsonSettings);
        Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonStorageUsage> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TStorageUsageStats>();
    }
};

template <>
struct TJsonRequestParameters<TJsonStorageUsage> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
              - name: enums
                in: query
                description: convert enums to strings
                type: boolean
                required: false
              - name: ui64
                in: query
                description: return ui64 as number
                type: boolean
                required: false
              - name: tenant
                in: query
                description: tenant name
                type: string
                required: false
              - name: pool
                in: query
                description: storage pool name
                type: string
                required: false
              - name: node_id
                in: query
                description: node id
                type: integer
                required: false
              - name: pace
                in: query
                description: bucket size as a percentage
                type: integer
                required: false
                default: 5
              - name: timeout
                in: query
                description: timeout in ms
                type: integer
                required: false
                )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonStorageUsage> {
    static TString GetSummary() {
        return "Storage groups statistics";
    }
};

template <>
struct TJsonRequestDescription<TJsonStorageUsage> {
    static TString GetDescription() {
        return "Returns the distribution of groups by usage";
    }
};

}
}

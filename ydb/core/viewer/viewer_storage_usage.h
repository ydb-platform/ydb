#pragma once
#include "json_handlers.h"
#include "json_storage_base.h"
#include "viewer_pdiskinfo.h"
#include "viewer_vdiskinfo.h"

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

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

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Storage groups statistics",
            .Description = "Returns the distribution of groups by usage",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "tenant",
            .Description = "tenant name",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "pool",
            .Description = "storage pool name",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "node_id",
            .Description = "node id",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "pace",
            .Description = "bucket size as a percentage",
            .Type = "integer",
            .Default = "5",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TStorageUsageStats>());
        return yaml;
    }
};

}

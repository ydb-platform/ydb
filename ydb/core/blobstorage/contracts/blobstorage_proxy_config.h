#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/stream/str.h>

#include <algorithm>
#include <span>

namespace NKikimr {

    struct TNodeLayoutInfo : TThrRefBase {
        TVector<ui32> DistancePerOrderNumber;
        bool HasAllDataCenters;

        TNodeLayoutInfo(const TNodeLocation& selfLocation, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const THashMap<ui32, TNodeLocation>& map)
            : DistancePerOrderNumber(info->GetTotalVDisksNum(), Max<ui32>())
            , HasAllDataCenters(!DistancePerOrderNumber.empty() && selfLocation.HasKey(TNodeLocation::TKeys::DataCenter))
        {
            // In a physical pile group, the pile name identifies the copy and can span several datacenters.
            const bool skipPile = info->GetBridgeProxyGroupId().has_value();
            auto items = [skipPile](const TNodeLocation& location) {
                std::span result(location.GetItems());
                return skipPile && !result.empty() && result.front().first == TNodeLocation::TKeys::BridgePileName
                    ? result.subspan(1) : result;
            };
            const auto self = items(selfLocation);
            for (ui32 i = 0; i < DistancePerOrderNumber.size(); ++i) {
                if (const auto it = map.find(info->GetActorId(i).NodeId()); it != map.end()) {
                    const auto disk = items(it->second);
                    const auto [end, _] = std::ranges::mismatch(self, disk);
                    if (end != self.begin()) {
                        DistancePerOrderNumber[i] -= std::prev(end)->first;
                    }
                    HasAllDataCenters &= it->second.HasKey(TNodeLocation::TKeys::DataCenter);
                } else {
                    HasAllDataCenters = false;
                }
            }
        }
    };

    using TNodeLayoutInfoPtr = TIntrusivePtr<TNodeLayoutInfo>;

    struct TEvBlobStorage::TEvConfigureProxy
        : public TEventLocal<TEvBlobStorage::TEvConfigureProxy, TEvBlobStorage::EvConfigureProxy>
    {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TNodeLayoutInfoPtr NodeLayoutInfo;
        TIntrusivePtr<TStoragePoolCounters> StoragePoolCounters;

        TEvConfigureProxy(TIntrusivePtr<TBlobStorageGroupInfo> info, TNodeLayoutInfoPtr nodeLayoutInfo,
                TIntrusivePtr<TStoragePoolCounters> storagePoolCounters = nullptr)
            : Info(std::move(info))
            , NodeLayoutInfo(std::move(nodeLayoutInfo))
            , StoragePoolCounters(std::move(storagePoolCounters))
        {}

        TString ToString() const override {
            TStringStream str;
            str << "{TEvConfigureProxy Info# ";
            if (Info) {
                str << Info->ToString();
            } else {
                str << "nullptr";
            }
            str << " StoragePoolCounters# ";
            if (StoragePoolCounters) {
                str << "specified";
            } else {
                str << "nullptr";
            }
            str << "}";
            return str.Str();
        }
    };

} // namespace NKikimr

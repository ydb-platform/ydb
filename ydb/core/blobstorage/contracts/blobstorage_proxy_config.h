#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/stream/str.h>

namespace NKikimr {

    struct TNodeLayoutInfo : TThrRefBase {
        // indexed by NodeId
        TNodeLocation SelfLocation;
        TVector<TNodeLocation> LocationPerOrderNumber;

        TNodeLayoutInfo(const TNodeLocation& selfLocation, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                THashMap<ui32, TNodeLocation>& map)
            : SelfLocation(selfLocation)
            , LocationPerOrderNumber(info->GetTotalVDisksNum())
        {
            for (ui32 i = 0; i < LocationPerOrderNumber.size(); ++i) {
                LocationPerOrderNumber[i] = map[info->GetActorId(i).NodeId()];
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

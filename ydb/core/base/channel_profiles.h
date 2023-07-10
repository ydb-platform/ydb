#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>

namespace NKikimr {

struct TChannelProfiles : public TThrRefBase {
    struct TProfile : public TThrRefBase {
        struct TChannel {
            TBlobStorageGroupType::EErasureSpecies Erasure;
            ui64 PDiskCategory;
            NKikimrBlobStorage::TVDiskKind::EVDiskKind VDiskCategory;

            TString PoolKind;

            TChannel(TBlobStorageGroupType::EErasureSpecies erasure,
                     ui64 pDiskCategory,
                     NKikimrBlobStorage::TVDiskKind::EVDiskKind vDiskCategory,
                     TString poolKind = TString())
                : Erasure(erasure)
                , PDiskCategory(pDiskCategory)
                , VDiskCategory(vDiskCategory)
                , PoolKind(poolKind)
            {}

            bool operator ==(const TChannel& a) const {
                return Erasure == a.Erasure
                        && PDiskCategory == a.PDiskCategory
                        && VDiskCategory == a.VDiskCategory
                        && PoolKind == a.PoolKind;
            }

            bool operator !=(const TChannel& a) const {
                return !operator ==(a);
            }
        };

        TVector<TChannel> Channels;

        template<typename T>
        TProfile(const T &channels)
            : Channels(channels.begin(), channels.end())
        {}

        TProfile()
        {}
    };

    TVector<TProfile> Profiles;

    template<typename T>
    TChannelProfiles(const T &profiles)
        : Profiles(profiles.begin(), profiles.end())
    {}

    TChannelProfiles()
    {}
};

}

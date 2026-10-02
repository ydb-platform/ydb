#pragma once

// Lightweight tablet-storage descriptor types split out of
// ydb/core/base/blobstorage.h so that widely-included base headers can
// depend on TTabletStorageInfo / TTabletChannelInfo / TGroupID /
// TStorageStatusFlags without pulling in the heavy TEvBlobStorage event
// definitions. blobstorage.h includes this header, so existing users keep
// working unchanged.

#include "defs.h"

#include "blobstorage_pdisk_category.h"
#include "boot_type.h"
#include "tablet_types.h"
#include "logoblob.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/blobstorage_base.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/datetime/base.h>
#include <util/stream/str.h>
#include <util/generic/xrange.h>

namespace NKikimr {

struct TStorageStatusFlags {
    ui32 Raw = 0;

    TStorageStatusFlags()
    {}

    TStorageStatusFlags(ui32 raw)
        : Raw(raw)
    {}

    TStorageStatusFlags(const TStorageStatusFlags&) = default;
    TStorageStatusFlags& operator =(const TStorageStatusFlags&) = default;

    friend bool operator ==(const TStorageStatusFlags& x, const TStorageStatusFlags& y) { return x.Raw == y.Raw; }
    friend bool operator !=(const TStorageStatusFlags& x, const TStorageStatusFlags& y) { return x.Raw != y.Raw; }

    void Merge(ui32 raw) {
        if (raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
            Raw |= (raw & (
                ui32(NKikimrBlobStorage::StatusIsValid)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceOrange)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceRed)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceBlack)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceCyan)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceLightOrange)
                | ui32(NKikimrBlobStorage::StatusDiskSpacePreOrange)));
        }
    }

    bool Check(NKikimrBlobStorage::EStatusFlags statusToCheck) const {
        return (Raw & ui32(NKikimrBlobStorage::StatusIsValid)) && (Raw & ui32(statusToCheck));
    }

    TString ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    void Output(IOutputStream &out) const {
        out << "{"
            << ((Raw & NKikimrBlobStorage::StatusIsValid) ? " Valid" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceCyan) ? " Cyan" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) ? " LightYellow" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceYellowStop) ? " Yellow" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceLightOrange) ? " LightOrange" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpacePreOrange) ? " PreOrange" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceOrange) ? " Orange" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceRed) ? " Red" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceBlack) ? " Black" : "")
            << " }";
    }
};

NKikimrBlobStorage::EPDiskType PDiskTypeToPDiskType(const NPDisk::EDeviceType type);

NPDisk::EDeviceType PDiskTypeToPDiskType(const NKikimrBlobStorage::EPDiskType type);

enum class EGroupConfigurationType : ui32 {
    Static = 0,
    Dynamic = 1,
    Virtual = 2,
};

struct TGroupID {
    TGroupID() = default;
    TGroupID(const TGroupID&) = default;
    TGroupID(const TGroupId wrappedId)
        : Raw(wrappedId.GetRawId()) {}

    TGroupID(EGroupConfigurationType configurationType, ui32 dataCenterId, ui32 groupLocalId) {
        Set(configurationType, dataCenterId, groupLocalId);
    }

    explicit TGroupID(ui32 raw)
        : Raw(raw)
    {}

    EGroupConfigurationType ConfigurationType() const {
        const auto type = static_cast<EGroupConfigurationType>(Raw >> TypeShift & TypeMask);
        if (type == EGroupConfigurationType::Static) {
            return type;
        } else {
            const ui32 domainId = Raw >> DomainShift & DomainMask;
            return domainId == VirtualGroupDomain
                ? EGroupConfigurationType::Virtual
                : EGroupConfigurationType::Dynamic;
        }
    }

    ui32 AvailabilityDomainID() const {
        const auto type = static_cast<EGroupConfigurationType>(Raw >> TypeShift & TypeMask);
        const ui32 domainId = Raw >> DomainShift & DomainMask;
        return type == EGroupConfigurationType::Static ? domainId :
            domainId == VirtualGroupDomain ? 1 :
            domainId;
    }

    ui32 GroupLocalID() const {
        return Raw & GroupMask;
    }

    ui32 GetRaw() const {
        return Raw;
    }

    friend bool operator ==(const TGroupID& x, const TGroupID& y) { return x.Raw == y.Raw; }
    friend bool operator !=(const TGroupID& x, const TGroupID& y) { return x.Raw != y.Raw; }

    TGroupID& operator++() {
        Set(ConfigurationType(), AvailabilityDomainID(), NextValidLocalId());
        return *this;
    }

    TGroupID operator++(int) {
        TGroupID old(*this);
        ++*this;
        return old;
    }

    TString ToString() const;

private:
    static constexpr ui32 TypeWidth = 1;
    static constexpr ui32 TypeMask = (1 << TypeWidth) - 1;
    static constexpr ui32 TypeShift = 32 - TypeWidth;

    static constexpr ui32 DomainWidth = 6;
    static constexpr ui32 DomainMask = (1 << DomainWidth) - 1;
    static constexpr ui32 DomainShift = TypeShift - DomainWidth;
    static constexpr ui32 VirtualGroupDomain = DomainMask;
    static constexpr ui32 MaxValidDomain = DomainMask - 1;

    static constexpr ui32 GroupWidth = 25;
    static constexpr ui32 GroupMask = (1 << GroupWidth) - 1;
    static constexpr ui32 InvalidLocalId = GroupMask;
    static constexpr ui32 MaxValidGroup = GroupMask - 1;

    ui32 Raw = Max<ui32>();

    void Set(EGroupConfigurationType configurationType, ui32 availabilityDomainID, ui32 groupLocalId) {
        Y_ABORT_UNLESS(groupLocalId <= MaxValidGroup);

        switch (configurationType) {
            case EGroupConfigurationType::Static:
            case EGroupConfigurationType::Dynamic:
                Y_ABORT_UNLESS(availabilityDomainID <= MaxValidDomain);
                Raw = static_cast<ui32>(configurationType) << TypeShift | availabilityDomainID << DomainShift | groupLocalId;
                break;

            case EGroupConfigurationType::Virtual:
                Y_ABORT_UNLESS(availabilityDomainID == 1);
                Raw = static_cast<ui32>(EGroupConfigurationType::Dynamic) << TypeShift | VirtualGroupDomain << DomainShift | groupLocalId;
                break;
        }
    }

    ui32 NextValidLocalId() {
        const ui32 localId = GroupLocalID();
        return localId == InvalidLocalId ? localId :
            localId == MaxValidGroup ? 0 :
            localId + 1;
    }
};

// channel info for tablet
struct TTabletChannelInfo {
    struct THistoryEntry {
        ui32 FromGeneration;
        ui32 GroupID;
        TInstant Timestamp; // for diagnostics usage only

        THistoryEntry()
            : FromGeneration(0)
            , GroupID(0)
        {}

        THistoryEntry(ui32 fromGeneration, ui32 groupId, TInstant timestamp = TInstant()) // groupId could be zero
            : FromGeneration(fromGeneration)
            , GroupID(groupId)
            , Timestamp(timestamp)
        {}

        struct TCmp {
            bool operator()(ui32 gen, const THistoryEntry &x) const {
                return gen < x.FromGeneration;
            }
        };

        TString ToString() const {
            TStringStream str;
            str << "{FromGeneration# " << FromGeneration;
            str << " GroupID# " << GroupID;
            str << " Timestamp# " << Timestamp.ToString();
            str << "}";
            return str.Str();
        }

        bool operator ==(const THistoryEntry& other) const {
            return FromGeneration == other.FromGeneration
                    && (GroupID == other.GroupID || GroupID == 0 || other.GroupID == 0);
        }
    };

    ui32 Channel;
    TBlobStorageGroupType Type;
    TString StoragePool;
    TVector<THistoryEntry> History;

    TTabletChannelInfo()
        : Channel()
        , Type()
    {}

    TTabletChannelInfo(ui32 channel, TBlobStorageGroupType type)
        : Channel(channel)
        , Type(type)
    {}

    TTabletChannelInfo(ui32 channel, TBlobStorageGroupType::EErasureSpecies erasureSpecies)
        : Channel(channel)
        , Type(erasureSpecies)
    {}

    TTabletChannelInfo(ui32 channel, TString storagePool)
        : Channel(channel)
        , Type(TBlobStorageGroupType::ErasureNone)
        , StoragePool(storagePool)
    {}

    ui32 GroupForGeneration(ui32 gen) const {
        const size_t historySize = History.size();
        Y_ABORT_UNLESS(historySize > 0, "empty channel history");

        const THistoryEntry * const first = &*History.begin();
        if (historySize == 1) {
            if (first->FromGeneration <= gen)
                return first->GroupID;
            return Max<ui32>();
        }

        const THistoryEntry * const end = first + historySize;
        const THistoryEntry * const last = end - 1;
        if (last->FromGeneration <= gen) {
            return last->GroupID;
        }

        const THistoryEntry *x = UpperBound(first, end, gen, THistoryEntry::TCmp());
        if (x != first) {
            return (x - 1)->GroupID;
        }

        return Max<ui32>();
    }

    const THistoryEntry* LatestEntry() const {
        if (!History.empty())
            return &History.back();
        else
            return nullptr;
    }

    const THistoryEntry* PreviousEntry() const {
        if (History.size() > 1)
            return &*(History.rbegin() + 1);
        else
            return nullptr;
    }

    TString ToString() const {
        TStringStream str;
        str << "{Channel# " << Channel;
        str << " Type# " << Type.ToString();
        str << " StoragePool# " << StoragePool;
        str << " History# {";
        const size_t historySize = History.size();
        for (size_t historyIdx = 0; historyIdx < historySize; ++historyIdx) {
            if (historyIdx != 0) {
                str <<", ";
            }
            str << historyIdx << ":" << History[historyIdx].ToString();
        }
        str << "}";
        return str.Str();
    }
};

class TTabletStorageInfo : public TThrRefBase {
public:
    TTabletStorageInfo()
        : TabletID(Max<ui64>())
        , TabletType(TTabletTypes::TypeInvalid)
        , Version(0)
        , BootType(ETabletBootType::Normal)
    {}
    TTabletStorageInfo(ui64 tabletId, TTabletTypes::EType tabletType)
        : TabletID(tabletId)
        , TabletType(tabletType)
        , Version(0)
        , BootType(ETabletBootType::Normal)
    {}
    virtual ~TTabletStorageInfo() {}

    const TTabletChannelInfo* ChannelInfo(ui32 channel) const {
        if (Channels.size() <= channel) {
            return nullptr;
        }
        const TTabletChannelInfo &info = Channels[channel];
        if (info.History.empty()) {
            return nullptr;
        }
        return &info;
    }

    ui32 GroupFor(ui32 channel, ui32 recordGen) const {
        if (const TTabletChannelInfo *channelInfo = ChannelInfo(channel))
            return channelInfo->GroupForGeneration(recordGen);
        else
            return Max<ui32>();
    }

    ui32 GroupFor(const TLogoBlobID& id) const {
        return GroupFor(id.Channel(), id.Generation());
    }

    TString ToString() const {
        TStringStream str;
        str << "{Version# " << Version;
        str << " TabletID# " << TabletID;
        str << " TabletType# " << TabletType;
        str << " Channels# {";
        const size_t channelsSize = Channels.size();
        for (size_t channelIdx = 0; channelIdx < channelsSize; ++channelIdx) {
            if (channelIdx != 0) {
                str <<", ";
            }
            str << channelIdx << ":" << Channels[channelIdx].ToString();
        }
        str << "}";
        if (TenantPathId)
            str << " Tenant: " << TenantPathId;
        str << " BootType: " << BootType;
        return str.Str();
    }

    TActorId BSProxyIDForChannel(ui32 channel, ui32 generation) const;

    bool operator<(const TTabletStorageInfo &other) const noexcept {
        if (Version != 0 && other.Version != 0) {
            return Version < other.Version;
        }
        const size_t selfSize = Channels.size();
        const size_t otherSize = other.Channels.size();
        if (selfSize != otherSize)
            return (selfSize < otherSize);

        for (ui64 channelIdx : xrange(selfSize)) {
            const ui32 lastInSelf = Channels[channelIdx].History.back().FromGeneration;
            const ui32 lastInOther = other.Channels[channelIdx].History.back().FromGeneration;
            if (lastInSelf != lastInOther)
                return (lastInSelf < lastInOther);
        }

        return false;
    }

    //
    ui64 TabletID;
    TVector<TTabletChannelInfo> Channels;
    TTabletTypes::EType TabletType;
    ui32 Version;
    TPathId TenantPathId;
    ui64 HiveId = 0;
    ETabletBootType BootType = ETabletBootType::Normal;
};

inline TActorId TTabletStorageInfo::BSProxyIDForChannel(ui32 channel, ui32 generation) const {
    const ui32 group = GroupFor(channel, generation);
    Y_ABORT_UNLESS(group != Max<ui32>());
    const TActorId proxy = MakeBlobStorageProxyID(group);
    return proxy;
}

} // namespace NKikimr

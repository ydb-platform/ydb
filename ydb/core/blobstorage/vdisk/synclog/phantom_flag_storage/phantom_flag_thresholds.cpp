#include "phantom_flag_thresholds.h"

#include <algorithm>

namespace NKikimr {

namespace NSyncLog {


TPhantomFlagThresholds::TGenStep TPhantomFlagThresholds::MakeGenStep(const TLogoBlobID& blobId) {
    return TGenStep(blobId.Generation(), blobId.Step());
}

TPhantomFlagThresholds::TTabletChannel TPhantomFlagThresholds::MakeTabletChannel(const TLogoBlobID& blobId) {
    return TTabletChannel(blobId.TabletID(), blobId.Channel());
}

inline ui64 TPhantomFlagThresholds::THasher::operator()(const TTabletChannel& x) const {
    return std::hash<ui64>{}((x.first << 8) | x.second);
}

TPhantomFlagThresholds::TTabletThresholds::TTabletThresholds() {
    Thresholds.resize(MaxExpectedDisksInGroup);
}

void TPhantomFlagThresholds::TTabletThresholds::AddBlob(ui32 orderNumber, TGenStep genStep) {
    if (!Thresholds[orderNumber] || *Thresholds[orderNumber] < genStep) {
        DisksWithThreshold += static_cast<ui8>(!Thresholds[orderNumber]);
        Thresholds[orderNumber] = genStep;
    }
}

void TPhantomFlagThresholds::TTabletThresholds::AddBlob(TBlobStorageGroupType groupType, TGenStep genStep) {
    for (ui32 orderNumber = 0; orderNumber < groupType.BlobSubgroupSize(); ++orderNumber) {
        AddBlob(orderNumber, genStep);
    }
}

bool TPhantomFlagThresholds::TTabletThresholds::AddHardBarrier(ui32 orderNumber, TGenStep barrier) {
    if (Thresholds[orderNumber] && Thresholds[orderNumber] <= barrier) {
        Thresholds[orderNumber].reset();
        DisksWithThreshold -= 1;
    }
    return DisksWithThreshold > 0;
}

bool TPhantomFlagThresholds::TTabletThresholds::IsBehindThresholdOnUnsynced(TBlobStorageGroupType groupType,
        TGenStep genStep, const TSyncedMask& syncedMask) const {
    for (ui32 orderNumber = 0; orderNumber < groupType.BlobSubgroupSize(); ++orderNumber) {
        if (!syncedMask[orderNumber] && Thresholds[orderNumber] && *Thresholds[orderNumber] >= genStep) {
            return true;
        }
    }
    return false;
}

void TPhantomFlagThresholds::TTabletThresholds::Merge(TBlobStorageGroupType groupType, TTabletThresholds&& other) {
    for (ui32 orderNumber = 0; orderNumber < groupType.BlobSubgroupSize(); ++orderNumber) {
        if (other.Thresholds[orderNumber] &&
                (!Thresholds[orderNumber] || *Thresholds[orderNumber] < *other.Thresholds[orderNumber])) {
            DisksWithThreshold += !Thresholds[orderNumber];
            Thresholds[orderNumber] = other.Thresholds[orderNumber];
        }
    }
}

TString TPhantomFlagThresholds::TTabletThresholds::ToString(TBlobStorageGroupType groupType) const {
    TStringStream str;
    str << "{";
    str << " DisksWithThreshold# " << static_cast<ui32>(DisksWithThreshold);
    str << " Thresholds# [ ";
    for (ui32 orderNumber = 0; orderNumber < groupType.BlobSubgroupSize(); ++orderNumber) {
        const std::optional<TGenStep>& genstep = Thresholds[orderNumber];
        str << orderNumber << "#";
        if (genstep) {
            str << "[" << genstep->first << ":" << genstep->second << "] ";
        } else {
            str << "<none> ";
        }
    }
    str << "] }";
    return str.Str();
}

TPhantomFlagThresholds::TPhantomFlagThresholds(const TBlobStorageGroupType& gtype)
    : GType(gtype)
{}

void TPhantomFlagThresholds::AddBlob(const TLogoBlobID& blob) {
    TabletThresholds[MakeTabletChannel(blob)].AddBlob(GType, MakeGenStep(blob));
}

void TPhantomFlagThresholds::AddBlob(ui32 orderNumber, const TLogoBlobID& blob) {
    TabletThresholds[MakeTabletChannel(blob)].AddBlob(orderNumber, MakeGenStep(blob));
}

void TPhantomFlagThresholds::AddHardBarrier(ui32 orderNumber, ui64 tabletId, ui8 channel, ui32 generation, ui32 step) {
    TTabletChannel tabletChannel{tabletId, channel};
    if (auto it = TabletThresholds.find(tabletChannel); it != TabletThresholds.end()) {
        bool blobsRemain = it->second.AddHardBarrier(orderNumber, TGenStep(generation, step));
        if (!blobsRemain) {
            TabletThresholds.erase(it);
        }
    }
}

bool TPhantomFlagThresholds::IsBehindThresholdOnUnsynced(const TLogoBlobID& blob, const TSyncedMask& syncedMask) const {
    TTabletChannel tabletChannel = MakeTabletChannel(blob);
    if (auto it = TabletThresholds.find(tabletChannel); it != TabletThresholds.end()) {
        return it->second.IsBehindThresholdOnUnsynced(GType, MakeGenStep(blob), syncedMask);
    }
    return false;
}

TPhantomFlags TPhantomFlagThresholds::Sift(const TPhantomFlags& flags, const TSyncedMask& syncedMask) {
    TPhantomFlags res;
    std::copy_if(flags.begin(), flags.end(), std::back_inserter(res),
            [&](const TLogoBlobRec& rec) { return IsBehindThresholdOnUnsynced(rec.LogoBlobID(), syncedMask); });
    return res;
}

ui64 TPhantomFlagThresholds::EstimatedMemoryConsumption() const {
    ui64 res = sizeof(GType);
    res += sizeof(TabletThresholds);
    res += TabletThresholds.bucket_count() * sizeof(decltype(TabletThresholds)::value_type);
    res += TabletThresholds.size() * GType.BlobSubgroupSize() * sizeof(TGenStep);
    return res;
}

void TPhantomFlagThresholds::Merge(TPhantomFlagThresholds&& other) {
    for (auto& [tabletChannel, thresholds] : other.TabletThresholds) {
        auto it = TabletThresholds.find(tabletChannel);
        if (it == TabletThresholds.end()) {
            TabletThresholds[tabletChannel] = std::move(thresholds);
        } else {
            it->second.Merge(GType, std::move(thresholds));
        }
    }
}

void TPhantomFlagThresholds::Clear() {
    TabletThresholds.clear();
}

TString TPhantomFlagThresholds::ToString() const {
    TStringStream str;
    str << "TPhantomFlagThresholds# { ";
    for (const auto& [tabletChannel, thresholds] : TabletThresholds) {
        str << "[" << tabletChannel.first << ":" << static_cast<ui32>(tabletChannel.second) << "] ";
        str << thresholds.ToString(GType) << " ";
    }
    str << "}";
    return str.Str();
}

} // namespace NSyncLog

} // namespace NKikimr

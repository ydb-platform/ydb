#include "phantom_flag_thresholds.h"

#include <algorithm>

namespace NKikimr {

namespace NSyncLog {


void TPhantomFlagThresholds::TNeighbourThresholds::AddBlob(const TLogoBlobID& blob) {
    TabletThresholds[blob.TabletID()].AddBlob(blob);
}

bool TPhantomFlagThresholds::TNeighbourThresholds::IsBehindThreshold(const TLogoBlobID& blob) const {
    auto it = TabletThresholds.find(blob.TabletID());
    if (it == TabletThresholds.end()) {
        return false;
    }
    return it->second.IsBehindThreshold(blob);
}

void TPhantomFlagThresholds::TNeighbourThresholds::AddHardBarrier(ui64 tabletId, ui8 channel, TGenStep barrier) {
    auto it = TabletThresholds.find(tabletId);
    if (it == TabletThresholds.end()) {
        return;
    }

    TTabletThresholds& tabletThresholds = it->second;
    tabletThresholds.AddHardBarrier(channel, barrier);
    if (tabletThresholds.IsEmpty()) {
        TabletThresholds.erase(it);
    }
}

void TPhantomFlagThresholds::TNeighbourThresholds::TTabletThresholds::AddBlob(const TLogoBlobID& blob) {
    TGenStep& current = ChannelThresholds[blob.Channel()];
    current = std::max(current, TGenStep(blob.Generation(), blob.Step()));
}

void TPhantomFlagThresholds::TNeighbourThresholds::TTabletThresholds::AddHardBarrier(ui8 channel, TGenStep barrier) {
    auto it = ChannelThresholds.find(channel);
    if (it == ChannelThresholds.end()) {
        return;
    }

    if (it->second <= barrier) {
        ChannelThresholds.erase(it);
    }
}

bool TPhantomFlagThresholds::TNeighbourThresholds::TTabletThresholds::IsBehindThreshold(const TLogoBlobID& blob) const {
    auto it = ChannelThresholds.find(blob.Channel());
    if (it == ChannelThresholds.end()) {
        return false;
    }
    return it->second >= TGenStep(blob.Generation(), blob.Step());
}

bool TPhantomFlagThresholds::TNeighbourThresholds::TTabletThresholds::IsEmpty() const {
    return ChannelThresholds.empty();
}

TPhantomFlagThresholds::TPhantomFlagThresholds(const TBlobStorageGroupType& gtype)
    : GType(gtype)
    , NeighbourThresholds(GType.BlobSubgroupSize())
{}

void TPhantomFlagThresholds::AddBlob(ui32 orderNumber, const TLogoBlobID& blob) {
    if (orderNumber < GType.BlobSubgroupSize()) {
        NeighbourThresholds[orderNumber].AddBlob(blob);
    }
}

void TPhantomFlagThresholds::AddHardBarrier(ui32 orderNumber, ui64 tabletId, ui32 channel, ui32 generation, ui32 step) {
    if (orderNumber < GType.BlobSubgroupSize()) {
        NeighbourThresholds[orderNumber].AddHardBarrier(tabletId, channel, TGenStep{generation, step});
    }
}

bool TPhantomFlagThresholds::IsBehindThreshold(const TLogoBlobID& blob) const {
    return std::any_of(NeighbourThresholds.begin(), NeighbourThresholds.end(),
            [&](const TNeighbourThresholds& thresholds) {
                return thresholds.IsBehindThreshold(blob);
            }
    );
}

TPhantomFlags TPhantomFlagThresholds::Sift(const TPhantomFlags& flags) {
    TPhantomFlags res;
    std::copy_if(flags.begin(), flags.end(), res.begin(),
            [&](const TLogoBlobRec& rec) { return IsBehindThreshold(rec.LogoBlobID()); });
    return res;
}

} // namespace NSyncLog

} // namespace NKikimr

#include "reassign.h"
#include "hive_impl.h"

#include <util/generic/algorithm.h>

#include <algorithm>

namespace NKikimr::NHive {

TReassignTabletsPlan THive::PrepareReassignTablets(const TReassignTabletsSettings& settings) {
    TReassignTabletsPlan plan;
    if (!settings.ForcedGroupIds.empty() && settings.ForcedGroupIds.size() != settings.TabletChannels.size()) {
        plan.Error = "forcedGroup size should be equal to channel size";
        return plan;
    }
    if (!settings.ForcedGroupIds.empty() && settings.Async) {
        plan.Error = "cannot provide force groups in async mode";
        return plan;
    }

    for (const auto& [tabletId, tablet] : Tablets) {
        if (settings.TabletId && tabletId != *settings.TabletId) {
            continue;
        }
        if (settings.TabletType != TTabletTypes::TypeInvalid && tablet.Type != settings.TabletType) {
            continue;
        }
        TVector<ui32> channels;
        TVector<ui32> forcedGroupIds;
        if (settings.GroupId != 0 || !settings.StoragePool.empty()) {
            for (const auto& channel : tablet.TabletStorageInfo->Channels) {
                if (settings.StoragePool && channel.StoragePool != settings.StoragePool) {
                    continue;
                }
                if (settings.TabletChannels.empty() || Find(settings.TabletChannels, channel.Channel) != settings.TabletChannels.end()) {
                    const auto* latest = channel.LatestEntry();
                    if (latest != nullptr && (settings.GroupId == 0 || latest->GroupID == settings.GroupId)) {
                        channels.push_back(channel.Channel);
                        if (!settings.ForcedGroupIds.empty()) {
                            const auto it = Find(settings.TabletChannels, channel.Channel);
                            forcedGroupIds.push_back(settings.ForcedGroupIds[std::distance(settings.TabletChannels.begin(), it)]);
                        }
                    }
                }
            }
            if (channels.empty()) {
                continue;
            }
        } else {
            channels = settings.TabletChannels;
            forcedGroupIds = settings.ForcedGroupIds;
        }
        plan.Operations.emplace_back(tablet.Id, channels, forcedGroupIds, settings.Async);
    }

    TStringBuilder description;
    if (settings.TabletPercent != 100) {
        description << settings.TabletPercent << "% of ";
    }
    if (settings.TabletId) {
        description << *settings.TabletId << ", ";
    }
    if (settings.TabletType != TTabletTypes::TypeInvalid) {
        description << TTabletTypes::EType_Name(settings.TabletType) << ", ";
    }
    if (settings.StoragePool) {
        description << settings.StoragePool << ", ";
    }
    description << (settings.GroupId ? ToString(settings.GroupId) : "all groups");
    plan.Description = description;

    if (settings.TabletPercent != 100) {
        auto getMaxTimestamp = [this](const TReassignOperation& operation) {
            TInstant max;
            const auto* tablet = FindTablet(operation.TabletId);
            for (const auto& channel : tablet->TabletStorageInfo->Channels) {
                if (operation.TabletChannels.empty() || Find(operation.TabletChannels, channel.Channel) != operation.TabletChannels.end()) {
                    if (tablet->ChannelProfileNewGroup.test(channel.Channel)) {
                        return TInstant::Max();
                    }
                    const auto* latest = channel.LatestEntry();
                    if (latest != nullptr && latest->Timestamp > max) {
                        max = latest->Timestamp;
                    }
                }
            }
            return max;
        };
        std::sort(plan.Operations.begin(), plan.Operations.end(), [&](const auto& lhs, const auto& rhs) {
            return getMaxTimestamp(lhs) < getMaxTimestamp(rhs);
        });
        const auto it = plan.Operations.begin() + plan.Operations.size() * settings.TabletPercent / 100;
        plan.Operations.erase(it, plan.Operations.end());
    }

    if (settings.NumTablets && plan.Operations.size() > MAX_REASSIGNS_WITHOUT_CONFIRMATION
            && std::abs(static_cast<i64>(plan.Operations.size()) - *settings.NumTablets) > REASSIGN_CONFIRMATION_ERROR_MARGIN) {
        plan.Error = "must confirm number of tablets";
    }
    return plan;
}

} // namespace NKikimr::NHive

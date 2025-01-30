#include "hive_impl.h"
#include "hive_log.h"

template <>
inline IOutputStream& operator <<(IOutputStream& out, NKikimrHive::TEvReassignTablet::EHiveReassignReason reason) {
    return out << NKikimrHive::TEvReassignTablet::EHiveReassignReason_Name(reason);
}

namespace NKikimr {
namespace NHive {

class TTxUpdateTabletGroups : public TTransactionBase<THive> {
    TTabletId TabletId;
    TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> Groups;
    TSideEffects SideEffects;

public:
    TTxUpdateTabletGroups(TTabletId tabletId, TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> groups, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , Groups(std::move(groups))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_TABLET_GROUPS; }

    static bool MaySkipChannelReassign(const TLeaderTabletInfo* tablet, const TTabletChannelInfo* channel, const NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters* group) {
        if (tablet->ChannelProfileReassignReason == NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_BALANCE) {
            // Only a reassign for balancing may be skipped
            if (channel->History.back().GroupID == group->GetGroupID()) {
                // We decided to keep the group the same
                return true;
            }
            auto channelId = channel->Channel;
            auto tabletChannel = tablet->GetChannel(channelId);
            auto oldGroupId = channel->History.back().GroupID;
            auto& pool = tablet->GetStoragePool(channelId);
            auto& oldGroup = pool.GetStorageGroup(oldGroupId);
            auto& newGroup = pool.GetStorageGroup(group->GetGroupID());
            auto usageBefore = oldGroup.GetUsageForChannel(tabletChannel);
            auto usageAfter = newGroup.GetUsageForChannel(tabletChannel);
            if (usageAfter > usageBefore) {
                return true;
            }
        }
        return false;
    }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        SideEffects.Reset(Self->SelfId());

        ETabletState newTabletState = ETabletState::GroupAssignment;
        bool needToBlockStorage = false;
        bool needToIncreaseGeneration = false;
        bool changed = false;
        TStringBuilder tabletBootState;

        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (!tablet) {
            BLOG_W("THive::TTxUpdateTabletGroups:: tablet " << TabletId << " wasn't found");
            return true;
        }
        BLOG_D("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}("
               << tablet->Id << "," << tablet->ChannelProfileReassignReason << "," << Groups << ")");

        Y_ABORT_UNLESS(tablet->TabletStorageInfo);
        TIntrusivePtr<TTabletStorageInfo>& tabletStorageInfo(tablet->TabletStorageInfo);
        ui32 channels = tablet->GetChannelCount();
        NIceDb::TNiceDb db(txc.DB);

        if (tablet->ChannelProfileNewGroup.count() != Groups.size() && !Groups.empty()) {
            BLOG_ERROR("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                   << tablet->Id
                   << " ChannelProfileNewGroup has incorrect size");
            db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State>(ETabletState::ReadyToWork);
            tablet->State = ETabletState::ReadyToWork;
            tablet->TryToBoot();
            return true;
        }

        if (!tablet->ChannelProfileNewGroup.any()) {
            BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                   << tablet->Id
                   << " ChannelProfileNewGroup is empty");
            db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State>(ETabletState::ReadyToWork);
            tablet->State = ETabletState::ReadyToWork;
            tablet->TryToBoot();
            return true;
        }

        if (tablet->ChannelProfileReassignReason == NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_SPACE) {
            TInstant lastChangeTimestamp;

            for (ui32 channelId = 0; channelId < channels; ++channelId) {
                if (tablet->ChannelProfileNewGroup.test(channelId)) {
                    // searching for last change of this channel
                    if (tabletStorageInfo && tabletStorageInfo->Channels.size() > channelId
                        && tabletStorageInfo->Channels[channelId].History.size() > 1) {
                        TTabletChannelInfo::THistoryEntry& lastHistory = tabletStorageInfo->Channels[channelId].History.back();
                        lastChangeTimestamp = std::max(lastChangeTimestamp, lastHistory.Timestamp);
                    }
                }
            }

            TDuration timeSinceLastReassign = ctx.Now() - lastChangeTimestamp;
            if (lastChangeTimestamp && Self->GetMinPeriodBetweenReassign() && timeSinceLastReassign < Self->GetMinPeriodBetweenReassign()) {
                BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                    << tablet->Id
                    << " SpaceReassign was too soon - ignored");
                db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State>(ETabletState::ReadyToWork);
                tablet->State = ETabletState::ReadyToWork;
                tablet->TryToBoot();
                return true;
            }
        }

        // updating tablet channels
        TVector<TTabletChannelInfo>& tabletChannels = tablet->TabletStorageInfo->Channels;
        ui32 orderNumber = 0;
        for (ui32 channelId = 0; channelId < channels; ++channelId) {
            if (!tablet->ChannelProfileNewGroup.test(channelId)) {
                // we are skipping this channel because we haven't asked for it
                continue;
            }

            const NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters* group;

            if (Groups.size() > orderNumber) {
                group = &Groups[orderNumber];
            } else {
                group = tablet->FindFreeAllocationUnit(channelId);
                if (group == nullptr) {
                    BLOG_ERROR("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                            << tablet->Id
                            << " could not find a group for channel " << channelId
                            << " pool " << tablet->GetChannelStoragePoolName(channelId));
                    if (tabletBootState.empty()) {
                        tabletBootState << "Couldn't find a group for channel: ";
                        tabletBootState << channelId;
                    } else {
                        tabletBootState << ", ";
                        tabletBootState << channelId;
                    }
                    ++orderNumber;
                    continue;
                } else {
                    BLOG_D("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                        << tablet->Id
                        << " channel "
                        << channelId
                        << " assigned to group "
                        << group->GetGroupID());
                }
            }

            TTabletChannelInfo* channel;

            if (channelId < tabletChannels.size()) {
                channel = &tabletChannels[channelId];
                Y_ABORT_UNLESS(channel->Channel == channelId);
            } else {
                // increasing number of tablet channels
                tabletChannels.emplace_back();
                channel = &tabletChannels.back();
                channel->Channel = channelId;
            }

            if (MaySkipChannelReassign(tablet, channel, group)) {
                BLOG_D("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                    << tablet->Id
                    << " skipped reassign of channel "
                    << channelId);
                continue;
            }

            if (group->HasStoragePoolName()) {
                channel->StoragePool = group->GetStoragePoolName();
            } else if (group->HasErasureSpecies()) {
                channel->Type = TBlobStorageGroupType(static_cast<TErasureType::EErasureSpecies>(group->GetErasureSpecies()));
            } else {
                Y_ABORT_UNLESS(channelId < tablet->BoundChannels.size());
                auto& boundChannel = tablet->BoundChannels[channelId];
                channel->StoragePool = boundChannel.GetStoragePoolName();
            }

            db.Table<Schema::TabletChannel>().Key(tablet->Id, channelId).Update<Schema::TabletChannel::NeedNewGroup>(false);

            ui32 fromGeneration;
            if (channel->History.empty()) {
                fromGeneration = 0;
            } else {
                needToIncreaseGeneration = true;
                fromGeneration = tablet->KnownGeneration + 1;
            }

            if (!changed) {
                ++tabletStorageInfo->Version;
                db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::TabletStorageVersion>(tabletStorageInfo->Version);
            }

            TInstant timestamp = ctx.Now();
            db.Table<Schema::TabletChannelGen>().Key(tablet->Id, channelId, fromGeneration).Update(
                        NIceDb::TUpdate<Schema::TabletChannelGen::Group>(group->GetGroupID()),
                        NIceDb::TUpdate<Schema::TabletChannelGen::Version>(tabletStorageInfo->Version),
                        NIceDb::TUpdate<Schema::TabletChannelGen::Timestamp>(timestamp.MilliSeconds()));
            tablet->ReleaseAllocationUnit(channelId);
            if (!channel->History.empty() && fromGeneration == channel->History.back().FromGeneration) {
                channel->History.back().GroupID = group->GetGroupID(); // we overwrite history item when generation is the same as previous one (so the tablet didn't run yet)
                channel->History.back().Timestamp = timestamp;
            } else {
                auto& histogram = Self->TabletCounters->Percentile()[NHive::COUNTER_TABLET_CHANNEL_HISTORY_SIZE];
                if (channel->History.size() > 0) {
                    histogram.DecrementFor(channel->History.size());
                }
                channel->History.emplace_back(fromGeneration, group->GetGroupID(), timestamp);
                histogram.IncrementFor(channel->History.size());
            }
            if (channel->History.size() > 1) {
                // now we block storage for every change of a group's history
                needToBlockStorage = true;
            }
            changed = true;

            if (!tablet->AcquireAllocationUnit(channelId)) {
                BLOG_ERROR("Failed to aquire AU for tablet " << tablet->Id << " channel " << channelId);
            }
            tablet->ChannelProfileNewGroup.reset(channelId);

            ++orderNumber;
        }

        bool hasEmptyChannel = false;
        for (ui32 channelId = 0; channelId < channels; ++channelId) {
            if (tabletStorageInfo->Channels.size() <= channelId || tabletStorageInfo->Channels[channelId].History.empty()) {
                hasEmptyChannel = true;
                break;
            }
        }

        if (changed && (tablet->ChannelProfileNewGroup.none() || !hasEmptyChannel)) {
            if (tablet->ChannelProfileNewGroup.any()) {
                BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                       << tablet->Id
                       << " was partially changed");
            }

            for (ui32 channelId = 0; channelId < channels; ++channelId) {
                if (tablet->ChannelProfileNewGroup.test(channelId)) {
                    BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet " << tablet->Id
                           << " skipped channel " << channelId);
                    db.Table<Schema::TabletChannel>().Key(tablet->Id, channelId).Update<Schema::TabletChannel::NeedNewGroup>(false);
                    tablet->ChannelProfileNewGroup.reset(channelId);
                }
            }

            if (needToBlockStorage) {
                newTabletState = ETabletState::BlockStorage;
            } else {
                newTabletState = ETabletState::ReadyToWork;
            }

            if (tablet->IsBootingSuppressed()) {
                // Tablet will never boot, so will notify about creation right after commit
                for (const TActorId& actor : tablet->ActorsToNotify) {
                    SideEffects.Send(actor, new TEvHive::TEvTabletCreationResult(NKikimrProto::OK, TabletId));
                }
                tablet->ActorsToNotify.clear();
                db.Table<Schema::Tablet>().Key(TabletId).UpdateToNull<Schema::Tablet::ActorsToNotify>();
            }
        } else {
            BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                   << tablet->Id
                   << " wasn't changed");
            if (hasEmptyChannel) {
                // we can't continue with partial/unsuccessfull reassign on 0 generation
                newTabletState = ETabletState::GroupAssignment;
            } else {
                // we will continue to boot tablet even with unsuccessfull reassign
                for (ui32 channelId = 0; channelId < channels; ++channelId) {
                    if (tablet->ChannelProfileNewGroup.test(channelId)) {
                        BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet " << tablet->Id
                               << " skipped channel " << channelId);
                        db.Table<Schema::TabletChannel>().Key(tablet->Id, channelId).Update<Schema::TabletChannel::NeedNewGroup>(false);
                        tablet->ChannelProfileNewGroup.reset(channelId);
                    }
                }
                for (const TActorId& actor : tablet->ActorsToNotifyOnRestart) {
                    SideEffects.Send(actor, new TEvPrivate::TEvRestartCancelled(tablet->GetFullTabletId()));
                }
                newTabletState = ETabletState::ReadyToWork;
            }
        }

        db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State>(newTabletState);
        tablet->State = newTabletState;

        if (!tabletBootState.empty()) {
            tablet->BootState = tabletBootState;
        } else {
            tablet->BootState = {};
        }

        if (changed) {
            tablet->NotifyStorageInfo(SideEffects);
            if (tablet->IsReadyToBlockStorage()) {
                if (!tablet->InitiateBlockStorage(SideEffects)) {
                    BLOG_W("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Execute"
                            " - InitiateBlockStorage was not successfull");
                }
            } else if (tablet->IsReadyToWork()) {
                if (!tablet->InitiateStop(SideEffects)) {
                    BLOG_W("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Execute"
                            " - InitiateStop was not successfull");
                }
            } else if (tablet->IsBootingSuppressed()) {
                // Use best effort to kill currently running tablet
                SideEffects.Register(CreateTabletKiller(TabletId, /* nodeId */ 0, tablet->KnownGeneration));
            }
            SideEffects.Callback([counters = Self->TabletCounters] { counters->Cumulative()[NHive::COUNTER_TABLETS_STORAGE_REASSIGNED].Increment(1); });
        }
        if (needToIncreaseGeneration) {
            tablet->IncreaseGeneration();
            db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::KnownGeneration>(tablet->KnownGeneration);
        }
        if (!tablet->TryToBoot()) {
            BLOG_NOTICE("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Execute"
                        " - TryToBoot was not successfull");
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Complete SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateUpdateTabletGroups(TTabletId tabletId, TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> groups) {
    return new TTxUpdateTabletGroups(tabletId, std::move(groups), this);
}

} // NHive
} // NKikimr

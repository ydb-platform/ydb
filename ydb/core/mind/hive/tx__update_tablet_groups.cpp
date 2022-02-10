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
    ETabletState NewTabletState = ETabletState::GroupAssignment;
    bool NeedToBlockStorage = false;
    bool Changed = false;
    bool Ignored = false;
    TCompleteNotifications Notifications;
 
public:
    TTxUpdateTabletGroups(TTabletId tabletId, TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> groups, THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , Groups(std::move(groups))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_TABLET_GROUPS; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        TStringBuilder tabletBootState;

        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (!tablet) {
            BLOG_W("THive::TTxUpdateTabletGroups:: tablet " << TabletId << " wasn't found");
            Ignored = true;
            return true;
        } 
        BLOG_D("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}(" 
               << tablet->Id << "," << tablet->ChannelProfileReassignReason << "," << Groups << ")");
 
        Y_VERIFY(tablet->TabletStorageInfo);

        if (tablet->ChannelProfileNewGroup.count() != Groups.size() && !Groups.empty()) {
            BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                   << tablet->Id
                   << " ChannelProfileNewGroup has incorrect size");
            Ignored = true;
            return true;
        }

        if (!tablet->ChannelProfileNewGroup.any()) {
            BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                   << tablet->Id
                   << " ChannelProfileNewGroup is empty");
            Ignored = true;
            return true;
        }

        TIntrusivePtr<TTabletStorageInfo>& tabletStorageInfo(tablet->TabletStorageInfo);
        ui32 channels = tablet->GetChannelCount();

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
                NewTabletState = ETabletState::ReadyToWork;
                return true;
            }
        } 
 
        NIceDb::TNiceDb db(txc.DB);

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
                Y_VERIFY(channel->Channel == channelId);
                if (!tablet->ReleaseAllocationUnit(channelId)) {
                    BLOG_ERROR("Failed to release AU for tablet " << tablet->Id << " channel " << channelId);
                }
            } else {
                // increasing number of tablet channels
                tabletChannels.emplace_back();
                channel = &tabletChannels.back();
                channel->Channel = channelId;
            }

            if (group->HasStoragePoolName()) {
                channel->StoragePool = group->GetStoragePoolName();
            } else if (group->HasErasureSpecies()) {
                channel->Type = TBlobStorageGroupType(static_cast<TErasureType::EErasureSpecies>(group->GetErasureSpecies()));
            } else {
                Y_VERIFY(channelId < tablet->BoundChannels.size());
                auto& boundChannel = tablet->BoundChannels[channelId];
                channel->StoragePool = boundChannel.GetStoragePoolName();
            }

            db.Table<Schema::TabletChannel>().Key(tablet->Id, channelId).Update<Schema::TabletChannel::NeedNewGroup>(false);

            ui32 fromGeneration = channel->History.empty() ? 0 : (tablet->KnownGeneration + 1);
            TInstant timestamp = ctx.Now();
            db.Table<Schema::TabletChannelGen>().Key(tablet->Id, channelId, fromGeneration).Update(
                        NIceDb::TUpdate<Schema::TabletChannelGen::Group>(group->GetGroupID()),
                        NIceDb::TUpdate<Schema::TabletChannelGen::Version>(tabletStorageInfo->Version),
                        NIceDb::TUpdate<Schema::TabletChannelGen::Timestamp>(timestamp.MilliSeconds()));
            if (!channel->History.empty() && fromGeneration == channel->History.back().FromGeneration) {
                channel->History.back().GroupID = group->GetGroupID(); // we overwrite history item when generation is the same as previous one (so the tablet didn't run yet)
                channel->History.back().Timestamp = timestamp;
            } else {
                channel->History.emplace_back(fromGeneration, group->GetGroupID(), timestamp);
            }
            if (channel->History.size() > 1) {
                // now we block storage for every change of a group's history
                NeedToBlockStorage = true;
            }
            Changed = true;

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

        if (Changed && (tablet->ChannelProfileNewGroup.none() || !hasEmptyChannel)) {
            ++tabletStorageInfo->Version;
            db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::TabletStorageVersion>(tabletStorageInfo->Version);

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

            if (NeedToBlockStorage) {
                NewTabletState = ETabletState::BlockStorage;
            } else {
                NewTabletState = ETabletState::ReadyToWork;
            }

            if (tablet->IsBootingSuppressed()) {
                // Tablet will never boot, so will notify about creation right after commit
                for (const TActorId& actor : tablet->ActorsToNotify) {
                    Notifications.Send(actor, new TEvHive::TEvTabletCreationResult(NKikimrProto::OK, TabletId));
                }
                tablet->ActorsToNotify.clear();
                db.Table<Schema::Tablet>().Key(TabletId).UpdateToNull<Schema::Tablet::ActorsToNotify>();
            }
        } else { 
            BLOG_W("THive::TTxUpdateTabletGroups::Execute{" << (ui64)this << "}: tablet "
                   << tablet->Id
                   << " wasn't changed anything");
            if (hasEmptyChannel) {
                // we can't continue with partial/unsuccessfull reassign on 0 generation
                NewTabletState = ETabletState::GroupAssignment;
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
                NewTabletState = ETabletState::ReadyToWork;
            }
        } 
 
        db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State>(NewTabletState);

        if (!tabletBootState.empty()) {
            tablet->BootState = tabletBootState;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Ignored) {
            BLOG_NOTICE("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Complete"
                        " - Ignored transaction");
        } else {
            BLOG_D("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Complete");
            TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
            if (tablet != nullptr) {
                tablet->State = NewTabletState;
                if (Changed) {
                    tablet->NotifyStorageInfo(ctx);
                    Notifications.Send(ctx);
                    if (tablet->IsReadyToBlockStorage()) {
                        if (!tablet->InitiateBlockStorage()) {
                            BLOG_W("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Complete"
                                   " - InitiateBlockStorage was not successfull");
                        }
                    } else if (tablet->IsReadyToWork()) {
                        if (!tablet->InitiateStop()) {
                            BLOG_W("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Complete"
                                   " - InitiateStop was not successfull");                            
                        }
                    } else if (tablet->IsBootingSuppressed()) {
                        // Use best effort to kill currently running tablet
                        ctx.Register(CreateTabletKiller(TabletId, /* nodeId */ 0, tablet->KnownGeneration));
                    }
                }
                if (!tablet->TryToBoot()) {
                    BLOG_NOTICE("THive::TTxUpdateTabletGroups{" << (ui64)this << "}(" << TabletId << ")::Complete"
                                " - TryToBoot was not successfull");
                }
            }
            Self->ProcessBootQueue();
        }
    }
};

ITransaction* THive::CreateUpdateTabletGroups(TTabletId tabletId, TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> groups) {
    return new TTxUpdateTabletGroups(tabletId, std::move(groups), this);
} 
 
} // NHive
} // NKikimr
